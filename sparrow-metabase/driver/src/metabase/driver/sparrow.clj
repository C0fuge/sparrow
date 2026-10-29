(ns metabase.driver.sparrow
  "Native Metabase driver for the Sparrow Arrow Flight SQL server (DuckDB SQL). Metabase talks to the server through
  the Java bridge `sparrow.metabase.FlightSqlBridge` (Arrow Flight SQL client, no JDBC): sync metadata from
  `GetDbSchemas`/`GetTables`, queries planned in complete result mode and read as Arrow batches.

  Modelled on Metabase's own non-JDBC SQL driver `bigquery-cloud-sdk`. MBQL is compiled by the `:sql` parent with
  inline values (the server rejects bound parameters)."
  (:require
   [clojure.core.async :as a]
   [clojure.string :as str]
   [metabase.driver :as driver]
   [metabase.driver-api.core :as driver-api]
   [metabase.driver.connection :as driver.conn]
   [metabase.util.log :as log])
  (:import
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

(defmethod driver/mbql->native :sparrow
  [driver query]
  ;; the server rejects statement parameters (?), so values are written into the SQL text
  (binding [driver/*compile-with-inline-parameters* true]
    ((get-method driver/mbql->native :sql) driver query)))

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
    ;; native queries with Metabase parameters compile to `?` placeholders: never run them with values missing
    (throw (ex-info "Statement parameters are not supported by the Sparrow driver yet."
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
                              :upload-with-auto-pk             false
                              :uploads                         false
                              :actions                         false
                              :transforms/table                false
                              :transforms/python               false}]
  (defmethod driver/database-supports? [:sparrow feature] [_driver _feature _db] supported?))

;; DuckDB on the server keeps TIMESTAMP values as wall clock and TIMESTAMP WITH TIME ZONE in UTC
(defmethod driver/db-default-timezone :sparrow [_driver _database] "UTC")
