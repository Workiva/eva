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

(ns eva.server.v2
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.core.server]
            [clojure.string :as str]
            [eva.server.v1.http-status-endpoint :as http-status]
            [eva.server.v2.config :as config]
            [eva.server.v2.config-monitor.alpha :refer [monitor!]]
            [eva.v2.system.database-catalogue.core :refer [initialize-database]]
            [eva.v2.storage.value-store.concurrent :as vsc]
            [eva.v2.system.transactor.core :as transactor]
            [eva.v2.system.indexing.core :as indexing]
            [eva.v2.database.core :as database]
            [eva.utils.logging :refer [logged]]
            [barometer.core :as metrics]
            [barometer.aspects :refer [timed]]
            [recide.sanex.logging :as log]
            [eva.config :refer [config-strict]]
            [eva.utils.tracing :refer [construct-tracer]]
            [ichnaie.core :as cljt]
            [quartermaster.core :as qu]
            [clojure.tools.logging]
            [morphe.core :as d]
            [flowgraph.core :refer [flowgraph]]
            [flowgraph.edge :as edge]
            [flowgraph.protocols :refer [submit shutdown]]
            [clojure.data])
  (:refer-clojure :exclude [pmap])
  (:import (java.io PushbackReader)
           (com.codahale.metrics MetricRegistry JvmAttributeGaugeSet ScheduledReporter)
           (com.codahale.metrics.jvm MemoryUsageGaugeSet GarbageCollectorMetricSet FileDescriptorRatioGauge ThreadStatesGaugeSet ClassLoadingGaugeSet)
           (java.util.concurrent TimeUnit)))

(def ^:dynamic *debug-repl* (boolean (System/getenv "EVA_DEBUG_REPL")))
(def ^:dynamic *debug-repl-address* (or (System/getenv "EVA_DEBUG_REPL_ADDRESS")
                                        "localhost"))
(def ^:dynamic *debug-repl-port* (or (some-> (System/getenv "EVA_DEBUG_REPL_PORT")
                                             (Long/parseLong))
                                     5555))

(def ^:dynamic *debug-configs* (or (some-> (System/getenv "EVA_TRANSACTORS_CONFIG_DEBUG")
                                           (edn/read-string))
                                   false))

(def ^:dynamic *debug-configs-info* (atom {}))

(def ^:dynamic *log-config-changes* (or (some-> (System/getenv "EVA_TRANSACTORS_LOG_CONFIG_CHANGES")
                                                (edn/read-string))
                                        false))

(def ^:dynamic *graphite-url* (or (System/getenv "GRAPHITE_METRICS_URL")
                                  "localhost"))

(def ^:dynamic *graphite-port* (or (some-> (System/getenv "GRAPHITE_METRICS_URL")
                                           (Long/parseLong))
                                   2003))


(defn read-config [source] (binding [*read-eval* false] (read (PushbackReader. (io/reader source)))))
(defn load-config* [source] (binding [*ns* (the-ns 'eva.server.v2.config)]
                              (eval (read-config source))))
(defn load-config [source]
  (let [c (load-config* source)]
    (when *debug-configs*
      (swap! *debug-configs-info*
             assoc
             ::source source
             ::loaded c))
    c))

(defn transactors-config-path [] (System/getenv "EVA_TRANSACTORS_CONFIG"))

(def default-config-check-ms 300000)  ;; 5min

(def config-check-ms (if-some [v (System/getenv "EVA_TRANSACTORS_CONFIG_CHECK_PERIOD_MS")]
                       (Long/parseLong v)
                       default-config-check-ms))


(defn filter-by-type [configs t] (filter #(= (::config/type %) t) configs))

(d/defn ^{::d/aspects [(logged :info)]}
  start-db-process [transaction-config]
  (let [transactor (transactor/transactor ::server-node transaction-config)
        indexer    @(qu/acquire indexing/indexer-manager
                                ::server-node
                                transaction-config)]
    (assoc transaction-config ::transactor transactor ::indexer indexer)))

(defn- pmap [name f coll]
  (let [graph (flowgraph
               name
               {:source (edge/transform f :sink :coordinated? false)}
               {} ;; vertex-specs
               {} ;; constructed-args
               (config-strict :eva.startup.thread-count))
        results @(submit graph coll)]
    (shutdown graph)
    results))

(d/defn ^{::d/aspects [timed (logged :info)]} start-all-database-processes
  [transaction-configs]
  (pmap "start-transaction-nodes" start-db-process transaction-configs))

(defn- start-database-processes
  [transaction-configs]
  (Thread/sleep (config-strict :eva.server.startup-delay))
  (start-all-database-processes transaction-configs))

(defn start-status-server ;; TODO: configure, don't hard code
  [transaction-configs]
  (let [status-keys [::database/id]]
    (http-status/status-server (or (System/getenv "EVA_STATUS_HOST") "0.0.0.0")
                               (or (some-> (System/getenv "EVA_STATUS_PORT") (Integer/parseInt)) 9999)
                               (constantly 200) ; TODO figure out how to determine status across multiple transactors
                               (constantly {:transaction-processors-for
                                            (mapv #(select-keys % status-keys) transaction-configs)}))))

(defn comparable-transactor-configs [configs]
  (->> configs
       (filter #(= ::config/transaction-processor (::config/type %)))
       (map #(dissoc %
                     :eva.v2.system.transactor.core/id
                     :eva.v2.system.indexing.core/id))))

(defn configs-changed? [as bs]
  (not= (set (comparable-transactor-configs as))
        (set (comparable-transactor-configs bs))))

(defn diff-configs [curr-config new-config]
  (let [curr-config (comparable-transactor-configs curr-config)
        new-config (comparable-transactor-configs new-config)
        [in-curr in-new in-both] (clojure.data/diff (set curr-config)
                                                    (set new-config))]
    {:added     (map :eva.v2.database.core/id in-new)
     :removed   (map :eva.v2.database.core/id in-curr)
     :unchanged (map :eva.v2.database.core/id in-both)}))

(defn on-configs-change [curr-config new-config]
  (let [{:keys [added
                removed
                unchanged]} (diff-configs curr-config new-config)
        change-msg (format "Database configuration change detected: %s added, %s removed, %s unchanged;"
                           (count added) (count removed) (count unchanged))]

    (when *log-config-changes*
      (binding [*print-length* 50]
        (log/debug "Database-head configurations" " (" (count added) ") " "ADDED: " (pr-str added))
        (log/debug "Database-head configurations" " (" (count removed) ") " "REMOVED: " (pr-str removed))))

    (when *debug-configs*
      (swap! *debug-configs-info* update ::changed (fnil conj []) {:at    (java.util.Date.)
                                                                   :value new-config}))
    (if (some-> (System/getenv "EVA_TRANSACTORS_EXIT_ON_CONFIG_CHANGE")
                (edn/read-string))
      (let [msg (str change-msg " " "Exiting!")]
        (log/info msg)
        ;; also print to stdout in case logger isn't flushed
        (println msg)
        (System/exit 0))
      (log/info (str change-msg " " "please restart to enable new configuration.")))))

(defn on-monitor-error [err]
  (when *debug-configs*
    (swap! *debug-configs-info* update ::errors (fnil conj []) {:at (java.util.Date.)
                                                                :error err}))
  (clojure.tools.logging/warn
    err
    (str "config-monitor encountered error;"
         "the monitor task will retry in "
         config-check-ms
         "ms")))

(d/defn ^{::d/aspects [(logged :info)]}   initialize-db [db]
  (let [v (qu/acquire vsc/concurrent-value-store-manager [::database-initialize (::database/id db)] db)]
    (try (initialize-database v (::database/id db))
         (finally
           (qu/release v true)))))

(d/defn ^{::d/aspects [timed (logged :info)]} initialize-dbs [dbs]
  (pmap "initialize-dbs" initialize-db dbs))

(defn start-transactors-node
  ([]
   (let [config-path (transactors-config-path)]
     (when-not config-path (throw (IllegalStateException. "EVA_TRANSACTORS_CONFIG is not set!")))
     (start-transactors-node config-path)))
  ([config-path]
   (let [configs (load-config config-path)
         configs-monitor (delay
                          (log/info "Starting config monitor")
                          (monitor! config-check-ms
                                    #'configs-changed?
                                    configs
                                    #(load-config config-path)
                                    #'on-configs-change
                                    #'on-monitor-error))
         ensure-dbs (filter-by-type configs ::config/ensure-database)
         transaction-configs (filter-by-type configs ::config/transaction-processor)]
     (initialize-dbs ensure-dbs)
     (let [status-server (start-status-server transaction-configs)
           result {::database-processes (start-database-processes transaction-configs)
                   ::status-server status-server
                   ::configs-monitor @configs-monitor}]
       result))))

(defn server-mode [] (some-> (System/getenv "EVA_SERVER_MODE") (str/upper-case)))

(defn server-mode-help []
  (println "Valid server modes are:")
  (println "- EVA_SERVER_MODE=TRANSACTORS_NODE :: starts a transactors host node"))

(def server-processes {})

;;
(defn start-metrics-reporter!
  ([]
   (let [env-report-period (some-> (System/getenv "EVA_METRIC_REPORT_PERIOD_SECONDS")
                                   (Integer/parseInt))
         report-period (or env-report-period 60)]
     (start-metrics-reporter! report-period)))
  ([seconds]
   (let [registry (doto ^MetricRegistry (metrics/default-registry)
                    (.register "jvm.mem" (MemoryUsageGaugeSet.))
                    (.register "jvm.gc" (GarbageCollectorMetricSet.))
                    (.register "jvm.file-descriptor-usage" (FileDescriptorRatioGauge.))
                    (.register "jvm.thread" (ThreadStatesGaugeSet.))
                    (.register "jvm.class-loading" (ClassLoadingGaugeSet.))
                    (.register "jvm.attrs" (JvmAttributeGaugeSet.)))
         ;; Results in log spam if metrics are not able to be published for one reason or another.
         ;; Graphite could be installed locally as described here https://graphite.readthedocs.io/en/latest/install.html
         ^ScheduledReporter reporter (or (metrics/graphite-reporter registry *graphite-url* *graphite-port* "eva")
                                         (metrics/console-reporter registry))]
     (.start reporter seconds TimeUnit/SECONDS)
     (alter-var-root #'server-processes merge {::metric-registry registry
                                               ::metric-reporter reporter}))))

(defn start-transactors! []
  (let [processes (start-transactors-node)]
    (alter-var-root #'server-processes merge processes)))

(defn start-tracing
  []
  (when (config-strict :eva.tracing)
    (cljt/set-global-tracer! (construct-tracer))
    (cljt/enable-tracing!)))

(defn start-server! [args]
  (try
    (when *debug-repl*
      (clojure.core.server/start-server {:address *debug-repl-address* :port *debug-repl-port* :name ::debug-repl :accept 'clojure.core.server/repl}))
    (condp = (server-mode)
      "TRANSACTORS_NODE" (do (start-tracing)
                             (start-metrics-reporter!)
                             (start-transactors!))
      nil (do (println "EVA_SERVER_MODE is not set!")
              (server-mode-help)
              (System/exit 1))
      (do (println "EVA_SERVER_MODE is invalid: " (server-mode))
          (server-mode-help)
          (System/exit 1)))))

(defn -main [& args]
  (try
    (start-server! args)
    (catch Throwable e
      (log/error e "Unhandled server error; aborting!")
      (System/exit 1))))

(comment
  (def config {:eva.v2.system.indexing.core/id
               #uuid "12efe0ab-3198-4d9a-96c2-5153099e651a",
               :eva.server.v2.config/type
               :eva.server.v2.config/transaction-processor,
               :eva.v2.messaging.address/transaction-publication
               "eva.v2.transacted.167885a0-0114-4f07-92c4-a2e4e8edf829.3abdb14e-1245-4212-ae79-6531c5df718b",
               :eva.v2.messaging.address/transaction-submission
               "eva.v2.transact.167885a0-0114-4f07-92c4-a2e4e8edf829.3abdb14e-1245-4212-ae79-6531c5df718b",
               :eva.v2.storage.block-store.impl.sql/db-spec
               {:subprotocol "mariadb",
                :subname "//localhost:3306/eva",
                :classname "org.mariadb.jdbc.Driver",
                :user "eva",
                :password "notasecret"},
               :eva.v2.database.core/id
               #uuid "3abdb14e-1245-4212-ae79-6531c5df718b",
               :eva.v2.storage.block-store.types/storage-type
               :eva.v2.storage.block-store.types/sql,
               :eva.v2.messaging.address/index-updates
               "eva.v2.index-updates.167885a0-0114-4f07-92c4-a2e4e8edf829.3abdb14e-1245-4212-ae79-6531c5df718b",
               :eva.v2.storage.value-store.core/partition-id
               #uuid "167885a0-0114-4f07-92c4-a2e4e8edf829",
               :broker-uri "tcp://localhost:61616?user=eva-peer&password=notasecret&retryInterval=1000&retryIntervalMultiplier=2.0&maxRetryInterval=60000&reconnectAttempts=-1",
               :eva.v2.system.transactor.core/id
               #uuid "aaa92662-5082-4823-beaf-05116d8d6bb4",
               :messenger-node-config/type :broker-uri
               :eva.v2.system.peer-connection.core/id (random-uuid)})
  (require 'eva.v2.system.peer-connection.core)
  (def conn (eva.v2.system.peer-connection.core/connect config)))
