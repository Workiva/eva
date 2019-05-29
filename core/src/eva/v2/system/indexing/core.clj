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

(ns eva.v2.system.indexing.core
  (:require [eva.config :refer [config-strict]]
            [eva.v2.database.core :as database]
            [eva.v2.database.index :refer [open-writable-indexes]]
            [eva.v2.database.log :refer [open-transaction-log log-entry set-log-count]]
            [eva.v2.storage.value-store.core :as values]
            [eva.sizing-api :as sapi]
            [eva.v2.storage.system :as storage]
            [eva.v2.storage.value-store.manager :as vs-manager]
            [eva.v2.system.protocols :as p]
            [eva.v2.system.indexing.indexer-agent :refer [indexer-agent] :as ia]
            [eva.v2.messaging.address :as address]
            [eva.v2.messaging.node.manager.alpha :as messenger-node]
            [eva.v2.messaging.node.manager.types :as messenger]
            [eva.v2.system.database-catalogue.core :as dbcat]
            [eva.v2.utils.spec :refer [conform-spec]]
            [eva.utils.logging :refer [logged]]
            [utiliva.core :refer [map-vals]]
            [eva.error :refer [error?]]
            [barometer.aspects :refer [timed concurrency-measured]]
            [quartermaster.core :as qu]
            [morphe.core :as d]
            [ichnaie.core :refer [traced]]
            [clojure.spec.alpha :as s]
            [com.stuartsierra.component :as c]
            [plumbing.core :as pc]
            [recide.core :refer [try*]]
            [recide.sanex.logging :as log]))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

(s/def ::id uuid?)

(s/def ::config
  (s/merge ::storage/config
           ::messenger/config
           (s/keys :req [::id
                         ::address/transaction-publication
                         ::address/index-updates])))

(s/def ::index-updates map?) ;; TODO: specify better

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;

(defn indexer-agents->pending-updates [indexer-agents]
  (into {}
        (filter (fn [[_ up]] (some? up)))
        (for [[index-name agent] indexer-agents]
          [index-name
           (:pending @agent)])))

(defn flush-ready? [overlay-size tx-delta]
  (let [max-span (config-strict :eva.database.indexes.max-tx-delta)
        max-size-bytes (* 1024 1024 (config-strict :eva.database.overlay.max-size-mb))]
    (and (> tx-delta 0)
         (cond
           (<= max-span tx-delta)
           (do (log/debug "advancing indexes, tx-delta:" tx-delta max-span)
               true)
           (<= max-size-bytes overlay-size)
           (do (log/debug "advancing indexes, overlay size:" overlay-size)
               true)
           :else
           false))))

(def ^:dynamic *restart-delay-ms* 1500)

(defn on-connection-error:try-restart-indexer [ex messenger messenger-id]
  "This is an error-listener function installed on the indexer's messenger.
  When a messaging connection encounters an exception, error-listener functions
  will be called. In the event of such a function error, function attempts to re-initiate
  the messenger *once*. If re-initiation fails then the JVM will exit (ie. fast fail), and
  an external process-manager is expected to restart the process."
  (future
    (log/warn ex "indexer messaging encountered error, attempting restart")
    (when (pos-int? *restart-delay-ms*) (Thread/sleep *restart-delay-ms*))
    (try* (qu/reinitiate messenger messenger-id)
          (catch (:not ::qu/no-such-handle) t
            (log/error t "indexer messaging restart encountered fatal error, aborting process")
            (System/exit -200))
          (catch Throwable t nil))))

(defn- onboard-messenger
  [{:as indexer :keys [indexer-id transaction-pub-addr index-updates-addr]}
   messenger-reference]
  (locking messenger-reference
    (let [messenger-snapshot (qu/resource* messenger-reference)
          messenger-id (qu/resource-id messenger-snapshot)
          indexer (assoc indexer :messenger messenger-reference)]
      (p/register-error-listener messenger-snapshot
                                 indexer-id
                                 on-connection-error:try-restart-indexer
                                 [messenger-reference messenger-id])
      (p/subscribe! messenger-snapshot
                    indexer-id
                    transaction-pub-addr
                    (fn outer-process-entry [entry] (p/process-entry indexer entry))
                    {})
      (p/open-publisher! messenger-snapshot index-updates-addr {}))))

(d/defn ^{::d/aspects [concurrency-measured traced (logged) timed]} process-entry-impl
  [{:as idxr :keys [database-id value-store database-info messenger index-updates-addr indexer-id]}
   {:as msg :keys [tx-num]}]
  (try
    (if-not (= database-id (:database-id msg))
      (log/warnf "indexer for %s ignoring broadcast from %s" database-id (:database-id msg))
      (let [tx-log-entry (-> value-store
                             (open-transaction-log database-info)
                             (set-log-count (inc tx-num)) ;; TODO: when would this be necessary?
                             (log-entry tx-num)
                             deref)
            res (if-let [pending (p/pending-updates? idxr tx-log-entry)]
                  pending
                  (-> idxr (p/advance-indexes! tx-log-entry) p/maybe-flush!))]
        (when (not-empty res)
          (log/debugf "indexer has pending updates: %s" res)
          (log/debug (format "indexer %s is publishing message %s" indexer-id {:index-updates res :database-id database-id}))
          (p/publish! @messenger
                      index-updates-addr
                      {:index-updates res :database-id database-id}))))
    (catch Throwable t
      (log/errorf t "indexer for %s encountered error processing entry." database-id)
      (throw t))))

(defn- clean-up-messaging
  [{:as indexer :keys [messenger transaction-pub-addr index-updates-addr]}]
  (p/unsubscribe! @messenger (qu/resource-id indexer) transaction-pub-addr)
  (p/close-publisher! @messenger index-updates-addr))

(defrecord Indexer [config
                    resource-id
                    database-id
                    indexer-id
                    database-info
                    transaction-pub-addr
                    index-updates-addr
                    value-store
                    messenger
                    indexer-agents]
  qu/SharedResource
  (initiate [this]
    (if (qu/initiated? this)
      this
      (qu/acquiring [_ (log/debug "starting indexer with:"
                                  {:database-id database-id
                                   :transaction-pub-addr transaction-pub-addr
                                   :index-updates-addr index-updates-addr})
                     resource-id (qu/new-resource-id)
                     value-store (qu/acquire vs-manager/value-store-manager resource-id config)
                     database-info (dbcat/database-info value-store database-id)
                     tx-log (open-transaction-log value-store database-info)
                     init-log-entry @(log-entry tx-log)
                     roots (:index-roots init-log-entry)
                     indexes (open-writable-indexes value-store roots)
                     indexer-agents (into {}
                                          (map-vals (partial indexer-agent database-id tx-log))
                                          indexes)
                     initiated-self (assoc this
                                           :resource-id (atom resource-id)
                                           :value-store value-store
                                           :database-info database-info
                                           :indexer-agents indexer-agents)
                     messenger (qu/acquire messenger-node/messenger-nodes resource-id config (partial onboard-messenger initiated-self))]
                    (assoc initiated-self :messenger messenger))))
  (terminate [this]
    (if (qu/terminated? this)
      this
      (let [res-id (qu/resource-id this)]
        (reset! resource-id nil)
        (qu/release value-store true)
        (try (clean-up-messaging this)
             (catch Throwable t)) ;; swallow exceptions
        (qu/release messenger true)
        (assoc this
               :value-store nil
               :database-info nil
               :messenger nil
               :indexer-agents nil))))
  (force-terminate [this]
    (if (qu/terminated? this)
      this
      (do (qu/force-terminate value-store)
          (qu/force-terminate messenger)
          (qu/terminate this))))
  (resource-id [_] (some-> resource-id deref))
  (initiated? [this] (boolean (qu/resource-id this)))
  (status* [this] {})
  p/Indexer
  (pending-updates? [this tx-log-entry]
    (qu/ensure-initiated! this "cannot update indexes.")
    (let [log-index-roots (:index-roots tx-log-entry)
          pending-updates (indexer-agents->pending-updates indexer-agents)
          pending-txs (into {} (map-vals :tx-num) pending-updates)
          log-txs (into {} (map-vals :tx-num) log-index-roots)]
      (when (some identity (for [[idx tx] pending-txs]
                             (> tx (get log-txs idx))))
        pending-updates)))
  (maybe-flush! [this]
    (qu/ensure-initiated! this "can't flush indexes.")
    (let [[max-delta size] (->> (vals indexer-agents)
                                (map (comp (juxt ia/tx-delta sapi/ram-size) deref))
                                (reduce (fn [[base size] [base' size']]
                                          [(max base base') (+ size size')])))]
      (when (flush-ready? size max-delta)
        (log/debug "Starting index flush")
        (let [agts (doall (for [[_ index-agent] indexer-agents]
                            (send-off index-agent ia/flush!)))]
          (apply await agts)
          (log/debug "Index flush complete")
          (indexer-agents->pending-updates indexer-agents)))))

  (advance-indexes! [this tx-log-entry]
    (qu/ensure-initiated! this "can't advance indexes.")
    (log/debug (format "indexer processing tx-log-entry %s" (into [] (eva.core/entry->datoms tx-log-entry))))
    (let [agts (doall (for [[_ index-agent] indexer-agents]
                        (send index-agent ia/advance-index tx-log-entry)))]
      (apply await agts)
      this))
  ;; TODO: re-inline this by adding options to the registration of the message handlers
  ;;       to inject timing / tracing / logging
  (process-entry [this {:as msg :keys [tx-num]}]
    (qu/ensure-initiated! this "can't process entry.")
    (log/debug (format "indexer %s got message %s" indexer-id msg))
    (process-entry-impl this msg)))

(defn indexer [{:as config}]
  {:pre [(conform-spec ::config config)]}
  (map->Indexer {:config config
                 :database-id (::database/id config)
                 :indexer-id (::id config)
                 :transaction-pub-addr (::address/transaction-publication config)
                 :index-updates-addr (::address/index-updates config)}))

(qu/defmanager indexer-manager
  :discriminator
  (fn [user-id config] [user-id (::database/id config) (::values/partition-id config) (::id config)])
  :constructor
  (fn [[user-id _ _] config] (indexer config)))
