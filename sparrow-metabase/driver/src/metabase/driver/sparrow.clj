(ns metabase.driver.sparrow
  "Native Metabase driver for the Sparrow Arrow Flight SQL server (DuckDB SQL). Metabase talks to the server through
  the Java bridge `sparrow.metabase.FlightSqlBridge` (Arrow Flight SQL client, no JDBC): sync metadata from
  `GetDbSchemas`/`GetTables`, queries planned in complete result mode and read as Arrow batches.

  Modelled on Metabase's own non-JDBC SQL driver `bigquery-cloud-sdk`. MBQL is compiled by the `:sql` parent with
  inline values (the server rejects bound parameters); temporal bucketing uses DuckDB's `date_trunc`/`extract`, with
  TIMESTAMP WITH TIME ZONE values bucketed in UTC (the database time zone) whatever the server session's zone."
  (:require
   [clojure.core.async :as a]
   [clojure.string :as str]
   [metabase.driver :as driver]
   [metabase.driver-api.core :as driver-api]
   [metabase.driver.common :as driver.common]
   [metabase.driver.connection :as driver.conn]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.util.honey-sql-2 :as h2x]
   [metabase.util.log :as log])
  (:import
   (java.time LocalDateTime LocalTime OffsetDateTime OffsetTime)
   (java.time.format DateTimeFormatter)
   (java.util Map)
   (sparrow.metabase Column FlightSqlBridge QueryResult TableRef)))

(set! *warn-on-reflection* true)

(driver/register! :sparrow, :parent #{:sql})

;;; ------------------------------------------------- Connection --------------------------------------------------

(defn- ->bridge-details
  "Metabase connection details (keyword keys) as the string-keyed map the bridge reads; nil values left out."
  ^Map [details]
  (into {} (keep (fn [[k v]] (when (some? v) [(name k) v]))) details))

(defn- connect
  "A bridge for connection `details` (client core caches the underlying connection, nothing needs closing)."
  ^FlightSqlBridge [details]
  (FlightSqlBridge/connect (->bridge-details details)))

(defn- database->bridge
  "A bridge for a Metabase database (its effective details, i.e. after database routing)."
  ^FlightSqlBridge [database]
  (connect (driver.conn/effective-details database)))

(defmethod driver/can-connect? :sparrow
  [_driver details]
  (.ping (connect details)))

;;; ---------------------------------------------------- Sync -----------------------------------------------------

(def ^:private database-type->base-type
  "DuckDB type names (as the bridge reports them) without parameters -> Metabase base types."
  {"BOOLEAN"                  :type/Boolean
   "TINYINT"                  :type/Integer
   "SMALLINT"                 :type/Integer
   "INTEGER"                  :type/Integer
   "UTINYINT"                 :type/Integer
   "USMALLINT"                :type/Integer
   "BIGINT"                   :type/BigInteger
   "UINTEGER"                 :type/BigInteger
   "UBIGINT"                  :type/BigInteger
   "FLOAT"                    :type/Float
   "DOUBLE"                   :type/Float
   "DECIMAL"                  :type/Decimal
   "VARCHAR"                  :type/Text
   "BLOB"                     :type/*
   "DATE"                     :type/Date
   "TIME"                     :type/Time
   "TIMESTAMP"                :type/DateTime
   "TIMESTAMP_S"              :type/DateTime
   "TIMESTAMP_MS"             :type/DateTime
   "TIMESTAMP_NS"             :type/DateTime
   "TIMESTAMP WITH TIME ZONE" :type/DateTimeWithLocalTZ
   ;; the bridge returns intervals and nested values as text (JSON for nested values)
   "INTERVAL"                 :type/Text})

(defn base-type
  "Metabase base type of a DuckDB type name such as `DECIMAL(10,2)`, `INTEGER[]` or `STRUCT(\"a\" INTEGER)`.
  Lists, structs and maps arrive as text (the bridge renders them as JSON); unknown types are `:type/*`."
  [^String database-type]
  (let [bare (str/replace database-type #"\(.*" "")]
    (cond
      (str/ends-with? database-type "]")      :type/Text
      (#{"STRUCT" "MAP" "UNION"} bare)       :type/Text
      :else                                  (get database-type->base-type bare :type/*))))

(defmethod driver/describe-database* :sparrow
  [_driver database]
  {:tables (into #{}
                 (map (fn [^TableRef t] {:name (.name t), :schema (.schema t)}))
                 (.tables (database->bridge database) nil))})

(defn- column->field
  "Sync metadata of a table column."
  [schema table ^Column column]
  {:table-schema      schema
   :table-name        table
   :name              (.name column)
   :database-type     (.databaseType column)
   :base-type         (base-type (.databaseType column))
   :database-position (.position column)
   :database-required false})

(defmethod driver/describe-fields :sparrow
  [_driver database & {:keys [schema-names table-names]}]
  (let [bridge  (database->bridge database)
        schemas (some-> schema-names set)
        names   (some-> table-names set)
        tables  (->> (.tables bridge nil)
                     (filter (fn [^TableRef t]
                               (and (or (nil? schemas) (contains? schemas (.schema t)))
                                    (or (nil? names) (contains? names (.name t))))))
                     (sort-by (fn [^TableRef t] [(.schema t) (.name t)])))]
    ;; ordered by schema, table and position, as describe-fields requires
    (eduction
     (mapcat (fn [^TableRef t]
               (map #(column->field (.schema t) (.name t) %)
                    (sort-by #(.position ^Column %) (.columns bridge (.schema t) (.name t))))))
     tables)))

;;; --------------------------------------------------- Queries ---------------------------------------------------

;; DuckDB's date_trunc('week', ...) starts weeks on Monday
(defmethod driver/db-start-of-week :sparrow [_driver] :monday)

(defn- tz?
  "Whether `expr` is a TIMESTAMP WITH TIME ZONE expression."
  [expr]
  (h2x/database-or-effective-type-isa? expr "timestamp with time zone" :type/DateTimeWithTZ))

(defn- utc-wall-clock
  "`expr` as a plain TIMESTAMP: TIMESTAMP WITH TIME ZONE values as their UTC wall clock, so that truncation and
  extraction do not depend on the server session's time zone; other expressions unchanged."
  [expr]
  (if (tz? expr)
    (h2x/with-database-type-info [:timezone (h2x/literal "UTC") expr] "timestamp")
    expr))

(defn- time-expr?
  "Whether `expr` is a TIME expression."
  [expr]
  (h2x/database-or-effective-type-isa? expr "time" :type/Time))

(defn- extract-integer
  "The integer `part` of a date, timestamp or time `expr`."
  [part expr]
  (h2x/with-database-type-info [::h2x/extract part (utc-wall-clock expr)] "bigint"))

(defn- date-trunc
  "`expr` truncated to `unit`, keeping its type: DuckDB's date_trunc returns a TIMESTAMP for a DATE (cast back) and
  has no TIME variant (rebuilt with make_time); TIMESTAMP WITH TIME ZONE is truncated as UTC wall clock."
  [unit expr]
  (cond
    (time-expr? expr)
    (h2x/with-database-type-info
     [:make_time
      (extract-integer :hour expr)
      (if (= unit :minute) (extract-integer :minute expr) [:inline 0])
      [:inline 0.0]]
     "time")

    (h2x/database-or-effective-type-isa? expr "date" :type/Date)
    (h2x/cast "date" [:date_trunc (h2x/literal unit) expr])

    :else
    (h2x/with-database-type-info [:date_trunc (h2x/literal unit) (utc-wall-clock expr)] "timestamp")))

(defmethod sql.qp/date [:sparrow :default]          [_ _ expr] expr)
(defmethod sql.qp/date [:sparrow :minute]           [_ _ expr] (date-trunc :minute expr))
(defmethod sql.qp/date [:sparrow :hour]             [_ _ expr] (date-trunc :hour expr))
(defmethod sql.qp/date [:sparrow :day]              [_ _ expr] (date-trunc :day expr))
(defmethod sql.qp/date [:sparrow :month]            [_ _ expr] (date-trunc :month expr))
(defmethod sql.qp/date [:sparrow :quarter]          [_ _ expr] (date-trunc :quarter expr))
(defmethod sql.qp/date [:sparrow :year]             [_ _ expr] (date-trunc :year expr))
(defmethod sql.qp/date [:sparrow :second-of-minute] [_ _ expr] (extract-integer :second expr))
(defmethod sql.qp/date [:sparrow :minute-of-hour]   [_ _ expr] (extract-integer :minute expr))
(defmethod sql.qp/date [:sparrow :hour-of-day]      [_ _ expr] (extract-integer :hour expr))
(defmethod sql.qp/date [:sparrow :day-of-month]     [_ _ expr] (extract-integer :day expr))
(defmethod sql.qp/date [:sparrow :day-of-year]      [_ _ expr] (extract-integer :doy expr))
(defmethod sql.qp/date [:sparrow :month-of-year]    [_ _ expr] (extract-integer :month expr))
(defmethod sql.qp/date [:sparrow :quarter-of-year]  [_ _ expr] (extract-integer :quarter expr))
(defmethod sql.qp/date [:sparrow :year-of-era]      [_ _ expr] (extract-integer :year expr))
(defmethod sql.qp/date [:sparrow :week-of-year-iso] [_ _ expr] (extract-integer :week expr))

(defmethod sql.qp/date [:sparrow :week]
  [driver _ expr]
  (sql.qp/adjust-start-of-week driver (partial date-trunc :week) expr))

(defmethod sql.qp/date [:sparrow :day-of-week]
  [driver _ expr]
  ;; extract(dow ...) is Sunday (0) ... Saturday (6)
  (sql.qp/adjust-day-of-week driver
                             (h2x/+ (extract-integer :dow expr) 1)
                             (driver.common/start-of-week-offset-for-day :sunday)))

(defn- interval
  "A DuckDB interval literal such as `INTERVAL '-2 month'` (amount is a number, unit one of Metabase's units)."
  [amount unit]
  {:pre [(number? amount)
         (#{:millisecond :second :minute :hour :day :week :month :quarter :year} unit)]}
  [:raw (format "INTERVAL '%s %s'" amount (name unit))])

(defmethod sql.qp/add-interval-honeysql-form :sparrow
  [_driver hsql-form amount unit]
  (if (h2x/is-of-type? hsql-form "date")
    ;; date + interval is a TIMESTAMP in DuckDB
    (h2x/cast "date" (h2x/+ hsql-form (interval amount unit)))
    (cond-> (h2x/+ hsql-form (interval amount unit))
      (h2x/type-info hsql-form) (h2x/with-type-info (h2x/type-info hsql-form)))))

(defmethod sql.qp/current-datetime-honeysql-form :sparrow
  [_driver]
  (h2x/with-database-type-info [:now] "timestamp with time zone"))

(defmethod sql.qp/unix-timestamp->honeysql [:sparrow :seconds]
  [_driver _unit expr]
  (h2x/with-database-type-info [:to_timestamp expr] "timestamp with time zone"))

;; Literals DuckDB parses (Metabase's defaults write `timestamp with time zone '... +02:00'`, which DuckDB reads as a
;; zone name, and cut fractions to milliseconds).
(def ^:private ^DateTimeFormatter timestamp-format (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SSSSSS"))
(def ^:private ^DateTimeFormatter timestamptz-format (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss.SSSSSSxxx"))
(def ^:private ^DateTimeFormatter time-format (DateTimeFormatter/ofPattern "HH:mm:ss.SSSSSS"))
(def ^:private ^DateTimeFormatter timetz-format (DateTimeFormatter/ofPattern "HH:mm:ss.SSSSSSxxx"))

(defmethod sql.qp/inline-value [:sparrow LocalDateTime]
  [_driver ^LocalDateTime t]
  (format "TIMESTAMP '%s'" (.format timestamp-format t)))

(defmethod sql.qp/inline-value [:sparrow OffsetDateTime]
  [_driver ^OffsetDateTime t]
  (format "TIMESTAMPTZ '%s'" (.format timestamptz-format t)))

(defmethod sql.qp/inline-value [:sparrow LocalTime]
  [_driver ^LocalTime t]
  (format "TIME '%s'" (.format time-format t)))

(defmethod sql.qp/inline-value [:sparrow OffsetTime]
  [_driver ^OffsetTime t]
  (format "TIMETZ '%s'" (.format timetz-format t)))

(defmethod driver/mbql->native :sparrow
  [driver query]
  ;; the server rejects statement parameters (?), so values are written into the SQL text
  (binding [driver/*compile-with-inline-parameters* true]
    ((get-method driver/mbql->native :sql) driver query)))

(defmethod driver/substitute-native-parameters-in-stage-method :sparrow
  [driver query stage-number]
  ;; template tags are replaced by inline SQL literals (as when Metabase converts a question to SQL); whatever still
  ;; needs a bound value stays in :params and is refused by execute-reducible-query
  (binding [driver/*compile-with-inline-parameters* true]
    ((get-method driver/substitute-native-parameters-in-stage-method :sql) driver query stage-number)))

(defn- result-cols
  "Column metadata of a query result."
  [^QueryResult result]
  (mapv (fn [^Column c]
          {:name          (.name c)
           :database_type (.databaseType c)
           :base_type     (base-type (.databaseType c))})
        (.columns result)))

(defmethod driver/execute-reducible-query :sparrow
  [_driver {{sql :query, :keys [params]} :native} _context respond]
  (when (seq params)
    ;; parameters are inlined (substitute-native-parameters-in-stage-method); never run a query with values missing
    (throw (ex-info "Statement parameters (?) are not supported by the Sparrow server."
                    {:type driver-api/qp.error-type.unsupported-feature, :sql sql, :params params})))
  (let [database  (driver-api/database (driver-api/metadata-provider))
        canceled? (volatile! false)]
    (with-open [result (.execute (database->bridge database) ^String sql)]
      (when-let [canceled-chan (driver-api/canceled-chan)]
        (a/go
          (when (a/<! canceled-chan)
            (vreset! canceled? true)
            (log/debug "Query canceled, canceling the Flight stream")
            (.cancel ^QueryResult result))))
      (respond {:cols (result-cols result)}
               (reify clojure.lang.IReduceInit
                 (reduce [_ rf init]
                   (loop [acc init]
                     (cond
                       (reduced? acc)            @acc
                       @canceled?                acc
                       (.hasNext result)         (recur (rf acc (vec (.next result))))
                       :else                     acc))))))))

;;; ---------------------------------------------- Driver properties ----------------------------------------------

(doseq [[feature supported?] {:describe-fields                 true
                              ;; the server has no keys or constraints
                              :metadata/key-constraints        false
                              :database-routing                false
                              :connection-impersonation        false
                              :set-timezone                    false
                              ;; not implemented (yet) for DuckDB: regexextract, datetimeDiff, convertTimezone
                              :regex                           false
                              :datetime-diff                   false
                              :convert-timezone                false
                              :upload-with-auto-pk             false
                              :uploads                         false
                              :actions                         false
                              :transforms/table                false
                              :transforms/python               false}]
  (defmethod driver/database-supports? [:sparrow feature] [_driver _feature _db] supported?))

;; DuckDB on the server keeps TIMESTAMP values as wall clock and TIMESTAMP WITH TIME ZONE in UTC
(defmethod driver/db-default-timezone :sparrow [_driver _database] "UTC")
