;; Copyright 2015-2019 Workiva Inc.
;; 
;; Licensed under the Eclipse Public License 1.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at
;; 
;;      http://opensource.org/licenses/eclipse-1.0.php
;; 
;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(ns eva.config
  (:require [recide.core :refer [insist deferror-group]]
            [recide.sanex :as sanex]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [clojure.pprint])
  (:import (java.io File)
           (java.lang Runtime)))

(deferror-group config-error
                :eva.config.error
                (parse-failure "Eva config parse error" [:target-type :source-type :source])
                (key-not-found "No entry found for config key" [:config-key]))

(defmulti parse-as (fn [as-type val] [as-type (type val)]))
(defmethod parse-as :default [typ x] (if (isa? (type x) typ)
                                       x
                                       (raise-config-error
                                         :parse-failure (format "cannot parse %s as %s" (type x) typ)
                                         {:target-type typ,
                                          :source-type (type x),
                                          :source      x})))
(defmethod parse-as [nil :default] [_ x] x)
(defmethod parse-as [String String] [_ s] s)
(defmethod parse-as [Long String] [_ s] (Long/parseLong s))
(defmethod parse-as [Integer String] [_ s] (Integer/parseInt s))
(defmethod parse-as [Boolean String] [_ s] (contains? #{"true" "1"} (str/lower-case s)))
(defmethod parse-as [Double String] [_ s] (Double/parseDouble s))
(defmethod parse-as [File String] [_ s] (-> (io/file (System/getProperty "user.dir") s)
                                            (.getAbsoluteFile)))

(def ^:dynamic *system-properties* (into {} (System/getProperties)))
(defn reload-system-properties [] (alter-var-root #'*system-properties* (fn [_] (into {} (System/getProperties)))))

(def ^:dynamic *properties*
  {:eva.system.core-limit                                               {:type    Long
                                                                         :default (.. Runtime getRuntime availableProcessors)
                                                                         :env     "EVA_CONTAINER_CORE_LIMIT"
                                                                         :doc     "Sets the size of available processors -- in Java 8 the JVM can't tell it's in a container."}
   :eva.concurrent.flowgraph.thread-count                               {:type    Long
                                                                         :default (.. Runtime getRuntime availableProcessors)
                                                                         :env     "EVA_CONTAINER_CORE_LIMIT"
                                                                         :doc     "The number of threads used by each flowgraph graph."}
   :eva.startup.thread-count                                            {:type Long
                                                                         :default (.. Runtime getRuntime availableProcessors)
                                                                         :env "EVA_CONTAINER_CORE_LIMIT"
                                                                         :doc "How many transactor nodes could be started simulteniously."}
   :eva.v2.storage.serialization.thread-count                           {:type    Long
                                                                         :default 12
                                                                         :env     "EVA_STORAGE_SERIALIZATION_THREADS"
                                                                         :doc     "The number of threads used by the v2 global reader & writer graphs."}
   :eva.v2.storage.block-size                                           {:type    Long
                                                                         :default (* 64 1024)
                                                                         :env     "EVA_STORAGE_BLOCK_SIZE"
                                                                         :doc     "Sets the maximum size size of storage blocks."}
   :eva.v2.storage.value-cache-size                                     {:type    Long
                                                                         :default 1000
                                                                         :env     "EVA_STORAGE_VALUE_CACHE_SIZE"
                                                                         :doc     "Sets the size of the (deserialized) value cache: proper size driven by index paramaters."}
   :eva.v2.storage.index-cache-size                                     {:type    Long
                                                                         :default 20
                                                                         :env     "EVA_STORAGE_INDEX_CACHE_SIZE"
                                                                         :doc     "Sets the size of the in-mem index cache: proper size driven by index parameters."}

   :eva.v2.storage.max-request-cardinality                              {:type    Long
                                                                         :env     "EVA_STORAGE_MAX_REQUEST_CARDINALITY"
                                                                         :default 25}

   :eva.v2.storage.request-timeout-ms                                   {:type    Long
                                                                         :env     "EVA_STORAGE_REQUEST_TIMEOUT_MS"
                                                                         :default 10000}

   :eva.v2.storage.ddb.max-retries                                      {:type    Long
                                                                         :default 20
                                                                         :env     "EVA_STORAGE_DDB_MAX_RETRIES"
                                                                         :doc     "Number of times to attempt an operation that dynamo has partially processed."}
   :eva.v2.storage.sql.max-retries                                      {:type    Long
                                                                         :default 5
                                                                         :env     "EVA_STORAGE_SQL_MAX_RETRIES"
                                                                         :doc     "Number of times to attempt a sql operation that has failed."}
   :eva.v2.storage.sql.connection-pool.max-pool-size                    {:type    Integer
                                                                         :default (int 15)
                                                                         :env     "EVA_STORAGE_SQL_CONNECTION_POOL_MAX_POOL_SIZE"
                                                                         :doc     "Maximum number of Connections a pool will maintain at any given time."}
   :eva.v2.storage.sql.connection-pool.min-pool-size                    {:type    Integer
                                                                         :default (int 3)
                                                                         :env     "EVA_STORAGE_SQL_CONNECTION_POOL_MIN_POOL_SIZE"
                                                                         :doc     "Minimum number of Connections a pool will maintain at any given time."}
   :eva.v2.storage.sql.connection-pool.max-connection-age               {:type    Integer
                                                                         :default (int 0)
                                                                         :env     "EVA_STORAGE_SQL_CONNECTION_POOL_MAX_CONNECTION_AGE"
                                                                         :doc     "Seconds, effectively a time to live. A Connection older than max-connection-age will be destroyed and purged from the pool. Zero means no maximum absolute age is enforced"}
   :eva.v2.storage.sql.connection-pool.max-idle-time                    {:type    Integer
                                                                         :default (int (* 3 60 60)) ;; 3hr
                                                                         :env     "EVA_STORAGE_SQL_CONNECTION_POOL_MAX_IDLE_TIME"
                                                                         :doc     "Seconds a Connection can remain pooled but unused before being discarded. Zero means idle connections never expire."}
   :eva.v2.storage.sql.connection-pool.max-idle-time-excess-connections {:type    Integer
                                                                         :default (int (* 30 60)) ;; 30min
                                                                         :env     "EVA_STORAGE_SQL_CONNECTION_POOL_MAX_IDLE_TIME_EXCESS_CONNECTIONS"
                                                                         :doc     "Number of seconds that Connections in excess of minPoolSize should be permitted to remain idle in the pool before being culled."}
   :eva.v2.storage.sql.connection-pool.test-connection-on-checkout      {:type    Boolean
                                                                         :default false
                                                                         :env     "EVA_STORAGE_SQL_CONNECTION_POOL_TEST_ON_CHECKOUT"
                                                                         :doc     "If true, jdbc connections will be tested on checkout from the connection-pool"}
   :eva.v2.storage.sql.connection-pool.test-connection-on-checkin       {:type    Boolean
                                                                         :default true
                                                                         :env     "EVA_STORAGE_SQL_CONNECTION_POOL_TEST_ON_CHECKIN"
                                                                         :doc     "If true, jdbc connections will be tested on checkin to the connection-pool"}
   :eva.v2.storage.sql.connection-pool.idle-connection-test-period      {:type    Integer
                                                                         :default (int 120) ;; 2 min
                                                                         :env     "EVA_STORAGE_SQL_CONNECTION_POOL_IDLE_TEST_PERIOD"
                                                                         :doc     "If N > 0, idle jdbc connections will be tested every N seconds"}
   :eva.server.host                                                     {:type    String
                                                                         :default "localhost"
                                                                         :env     "EVA_SERVER_HOST"
                                                                         :doc     "Host-name or ip of the server; used to as the address to bind to."}
   :eva.server.alt-host                                                 {:type String
                                                                         :env  "EVA_SERVER_ALT_HOST"
                                                                         :doc  "(Optional) Public host-name to publish as discovery address."}
   :eva.server.port                                                     {:type    Long
                                                                         :default 5445
                                                                         :env     "EVA_SERVER_PORT"
                                                                         :doc     "Primary port the server listens on."}
   :eva.server.status-http-port                                         {:type Long
                                                                         :env  "EVA_SERVER_STATUS_HTTP_PORT"
                                                                         :doc  "Port the server http-status endpoint listens on."}
   :eva.server.h2-port                                                  {:type    Long
                                                                         :default 5447
                                                                         :env     "EVA_SERVER_H2_PORT"}
   :eva.server.h2-web-port                                              {:type    Long
                                                                         :default 5448
                                                                         :env     "EVA_SERVER_H2_WEB_PORT"}
   :eva.server.ssl                                                      {:type    Boolean
                                                                         :default false
                                                                         :env     "EVA_SERVER_SSL"}
   :eva.server.data-dir                                                 {:type    File
                                                                         :default (io/file (System/getProperty "user.dir") "data")
                                                                         :env     "EVA_SERVER_DATA_DIR"}
   :eva.database.overlay.estimate-interval-sec                          {:type    Long
                                                                         :default 30}
   :eva.database.overlay.estimate-delay-sec                             {:type    Long
                                                                         :default 0}
   :eva.database.overlay.max-size-mb                                    {:type    Long
                                                                         :env     "EVA_DATABASE_OVERLAY_MAX_SIZE_MB"
                                                                         :default 32}
   :eva.log.read-chunk-size                                             {:type    Long
                                                                         :default 10
                                                                         :doc     "When lazy reading a span from the transaction log, request from storage in subspans of this size."
                                                                         :env     "EVA_LOG_READ_CHUNK_SIZE"}
   :eva.log.read-chunks-ahead                                           {:type    Long
                                                                         :default 10
                                                                         :doc     "When lazy reading a span from the transaction log, asynchronously attempt to stay this many chunks ahead."
                                                                         :env     "EVA_LOG_READ_CHUNKS_AHEAD"}
   :eva.database.indexes.max-tx-delta                                   {:type    Long
                                                                         :env     "EVA_DATABASE_INDEXES_MAX_TX_DELTA"
                                                                         :default 100}
   :eva.server.startup-delay                                            {:type    Long
                                                                         :env     "EVA_SERVER_STARTUP_DELAY"
                                                                         :default 0}
   :eva.transact-timeout                                                {:type    Long
                                                                         :default 10000
                                                                         :env     "EVA_TRANSACT_TIMEOUT"}
   :eva.transaction-pipeline.compile-db-fns                             {:type    Boolean
                                                                         :default true
                                                                         :env     "EVA_TRANSACTION_PIPELINE_COMPILE_DB_FNS"
                                                                         :doc     "Enable or disable the compilation of :db.type/fn attributes upon transaction."}
   :eva.transaction-pipeline.clock-skew-window                          {:type    Long
                                                                         :default 256
                                                                         :env     "EVA_TRANSACTION_PIPELINE_CLOCK_SKEW_WINDOW"
                                                                         :doc     "The amount of clock skew that the transaction process is willing to tolerate in ms."}
   :eva.transaction-pipeline.byte-size-limit                            {:type    Long
                                                                         :default 1024
                                                                         :env     "EVA_TRANSACTION_BYTE_SIZE_LIMIT"
                                                                         :doc     "The largest value for a string (in bytes) or bytes attribute the transactor will accept."}
   :eva.transaction-pipeline.limit-byte-sizes                           {:type    Boolean
                                                                         :default false
                                                                         :env     "EVA_TRANSACTION_REJECT_BYTE_LIMITS"
                                                                         :doc     "When enabled, transactions over the byte size limit will be rejected with an exception."}
   :eva.query.memoization-cache                                         {:type    Long
                                                                         :default 100
                                                                         :doc     "The threshold used for caching query compilation"}
   :eva.query.trace-logging                                             {:type    Boolean
                                                                         :default false
                                                                         :env     "EVA_QUERY_TRACE_LOGGING"
                                                                         :doc     "Whether or not tracing occurs in the query library."}

   :eva.telemetry.enable-per-db-metrics                                 {:type    Boolean
                                                                         :default false
                                                                         :env     "EVA_ENABLE_PER_DB_METRICS"
                                                                         :doc     "Enables per-db metrics. Adding flag to disable until we have a not-horribly-expensive way to use them."}
   :eva.tracing.tag-inputs                                              {:type    Boolean
                                                                         :default false
                                                                         :env     "EVA_TAG_INPUTS"
                                                                         :doc     "Tag inputs to query/pull/pull-many on the trace of the aforementioned operation."}
   :eva.tracing                                                         {:type    Boolean
                                                                         :default true
                                                                         :env     "EVA_TRACING"
                                                                         :doc     "Enables tracing. NOTE: Changing this variable after loading code that traces will have NO EFFECT. It MUST be set to false by the time our other Clojure files are loaded."}
   :eva.concurrent.background-resource.max                              {:type    Long
                                                                         :default 24
                                                                         :env     "EVA_BACKGROUND_RESOURCE_MAP_THREAD_MAX"
                                                                         :doc     "Maximum number of threads in the global background-resource thread pool."}
   :eva.concurrent.background-resource.queue-size                       {:type    Long
                                                                         :default 48
                                                                         :doc     "Maximum size of queue for tasks realizing background resources."}
   :eva.concurrent.background-resource.thread-goal                      {:type    Long
                                                                         :default 2
                                                                         :doc     "The target number of threads to be devoted to any particular background-resource-map."}
   :eva.error.capture-insists                                           {:type    Boolean
                                                                         :default false
                                                                         :env     "EVA_CAPTURE_INSISTS"
                                                                         :doc     "Insist will capture the values of local bindings used in the failing statement."}
   :eva.error.always-sanitize-exceptions                                {:type    Boolean
                                                                         :default false
                                                                         :env     "EVA_ALWAYS_SANITIZE"
                                                                         :doc     "Exceptions thrown from API endpoints should always be sanitized. A safety precaution -- strict reliance on this feature is probably an anti-pattern."}})

(defn- property-default
  ([key] (property-default *properties* key))
  ([prop-defs key] (get-in prop-defs [(keyword (name key)) :default])))

(defn- property-env-key
  ([key] (property-env-key *properties* key))
  ([prop-defs key]
   (get-in prop-defs [(keyword (name key)) :env])))

(defn- property-type
  ([key] (property-type *properties* key))
  ([prop-defs key] (or (get-in prop-defs [(keyword (name key)) :type])
                       (type (property-default prop-defs key)))))

(defn- property
  ([key] (property *system-properties* key))
  ([properties key] (when-some [val (or (get properties (name key))
                                        (get properties (str key)))]
                      (parse-as (property-type key) val))))

(defn- env-property
  ([env-map prop-name]
   (some->> (property-env-key prop-name)
            (get env-map)
            (parse-as (property-type prop-name)))))

(def ^:private ^:dynamic *overrides* {})
(defn- override-value [key] (get *overrides* key ::override-not-found))

(defmacro with-overrides [m & body]
  (insist (map? m))
  `(with-redefs [*overrides* ~m] ~@body))

(defn config
  "Get a configuration value. Returns nil if the key is not found."
  ([key] (config *system-properties* key))
  ([props key] (config *properties* props key))
  ([prop-defs props key]
   (let [ov (override-value key)]
     (if (not= ov ::override-not-found)
       ov
       (let [p (property props key)
             e (env-property (System/getenv) key)]
         (cond (some? p) p
               (some? e) e
               :else (property-default prop-defs key)))))))

(defn config-strict
  "Get a configuration value. Throws if the key is not found."
  ([key] (config-strict *system-properties* key))
  ([props key] (config-strict *properties* props key))
  ([prop-defs props key]
   (if-some [val (config prop-defs props key)]
     val
     (raise-config-error :key-not-found
                         (pr-str key)
                         {:config-key       key
                          ::sanex/sanitary? true}))))

(def ^:private server-key-prefix "eva.server.")
(defn filter-name-prefix [prefix] (filter (fn [k] (.startsWith (name k) (str prefix)))))

(defn server-config-keys
  ([] (server-config-keys *properties*))
  ([prop-defs]
   (into #{} (filter-name-prefix server-key-prefix)
         (keys prop-defs))))

(defn select-config [f ks props]
  (into {} (for [k ks] [k (f props k)])))

(defn base-server-config [prop-defs props] (select-config config-strict (server-config-keys prop-defs) props))

(defn ^:private print-table
  "Prints a collection of maps in a textual table. Prints table headings
   ks, and then a line of output for each row, corresponding to the keys
   in ks. If ks are not specified, use the keys of the first item in rows."
  ([ks rows]
   (when (seq rows)
     (let [widths (map
                    (fn [k]
                      (apply max (count (str k)) (map #(count (str (get % k))) rows)))
                    ks)
           spacers (map #(apply str (repeat % "-")) widths)
           fmts (map #(str "%-" % "s") widths)
           fmt-row (fn [leader divider trailer row]
                     (str leader
                          (apply str (interpose divider
                                                (for [[col fmt] (map vector (map #(get row %) ks) fmts)]
                                                  (format fmt (str col)))))
                          trailer))]
       (println)
       (println (fmt-row "| " " | " " |" (zipmap ks ks)))
       (println (fmt-row "|-" "-|-" "-|" (zipmap ks spacers)))
       (doseq [row rows]
         (println (fmt-row "| " " | " " |" row))))))
  ([rows] (print-table (keys (first rows)) rows)))

(defn help [& [path]]
  (let [prop-rows (for [[k m] *properties*]
                    (assoc m :name (name k)))]
    (when path (io/make-parents path))
    (binding [*out* (if-not path *out* (io/writer path))]
      (try
        (println)
        (when-not path (println "Eva Configuration Properties:"))
        (print-table [:name :env :doc :default] (sort-by :name prop-rows))
        (println)
        (finally
          (when path
            (.close ^java.io.Writer *out*)))))))
