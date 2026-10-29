(ns metabase.driver.sparrow-jdbc
  "Perf baseline, not shipped: Metabase reaching the Sparrow server through the Arrow Flight SQL JDBC driver, i.e.
  the JDBC route the native `:sparrow` driver replaces. A `:sql-jdbc` driver (pooled JDBC connections, sync from
  `DatabaseMetaData`, rows read through `ResultSet`s) whose SQL is compiled by the `:sparrow` driver, so both drivers
  send the server the same SQL and a comparison measures only the transport. Values are inlined (the server rejects
  bound parameters), so queries run as plain JDBC statements. Planned in the server's default result mode (no
  `x-flight-result-mode` header): a single-node server only."
  (:require
   [clojure.string :as str]
   [metabase.driver :as driver]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync])
  (:import
   (java.sql Connection DriverManager ResultSet Statement)
   (java.util Properties)))

(set! *warn-on-reflection* true)

;; Netty 4.x in the JDBC driver (relocated) disables sun.misc.Unsafe on Java 25+ by default, which Arrow's allocator
;; needs (as for the native driver's bridge, see docs/metabase.md)
(when-not (System/getProperty "org.apache.arrow.driver.jdbc.shaded.io.netty.noUnsafe")
  (System/setProperty "org.apache.arrow.driver.jdbc.shaded.io.netty.noUnsafe" "false"))

(driver/register! :sparrow-jdbc, :parent #{:sql-jdbc})

(defn- sparrow
  "The native `:sparrow` driver (loaded on first use), which compiles the SQL of this driver."
  []
  (driver/the-initialized-driver :sparrow))

(defmethod sql-jdbc.conn/connection-details->spec :sparrow-jdbc
  [_driver {:keys [host port token]}]
  (cond-> {:classname     "org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver"
           :subprotocol   "arrow-flight-sql"
           :subname       (str "//" host ":" (or port 8815))
           :useEncryption "false"}
    (not (str/blank? token)) (assoc :token token)))

(defmethod driver/can-connect? :sparrow-jdbc
  [driver details]
  ;; `SELECT 1` as a plain statement: the server's CreatePreparedStatement only plans queries over a table, and the
  ;; `:sql-jdbc` default runs the test query as a prepared statement
  (let [{:keys [subprotocol subname], :as spec} (sql-jdbc.conn/connection-details->spec driver details)
        props (Properties.)]
    (doseq [[k v] (dissoc spec :classname :subprotocol :subname)]
      (.setProperty props (name k) (str v)))
    (with-open [conn ^Connection (DriverManager/getConnection (str "jdbc:" subprotocol ":" subname) props)
                stmt ^Statement (.createStatement conn)
                rs   ^ResultSet (.executeQuery stmt "SELECT 1")]
      (.next rs))))

(defmethod sql-jdbc.sync/database-type->base-type :sparrow-jdbc
  [_driver database-type]
  (sparrow)
  ((requiring-resolve 'metabase.driver.sparrow/base-type) (str/upper-case (name database-type))))

(defmethod driver/mbql->native :sparrow-jdbc
  [_driver query]
  (driver/mbql->native (sparrow) query))

(defmethod driver/substitute-native-parameters-in-stage-method :sparrow-jdbc
  [_driver query stage-number]
  (driver/substitute-native-parameters-in-stage-method (sparrow) query stage-number))

(defmethod driver/db-start-of-week :sparrow-jdbc [_driver] :monday)

(defmethod driver/db-default-timezone :sparrow-jdbc [_driver _database] "UTC")

(doseq [[feature supported?] {:describe-fields                 false
                              :metadata/key-constraints        false
                              :database-routing                false
                              :connection-impersonation        false
                              :set-timezone                    false
                              :regex                           false
                              :datetime-diff                   false
                              :convert-timezone                false
                              :upload-with-auto-pk             false
                              :uploads                         false
                              :actions                         false
                              :transforms/table                false
                              :transforms/python               false}]
  (defmethod driver/database-supports? [:sparrow-jdbc feature] [_driver _feature _db] supported?))
