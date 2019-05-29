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

(ns eva.v2.database.index-manager
  (:require [clojure.core.cache :as c]
            [eva.config :refer [config-strict]]
            [eva.error :refer [raise]]
            [recide.sanex :refer [sanitize]]
            [recide.sanex.logging :as log]
            [eva.v2.database.log :refer [set-log-count open-transaction-log]]
            [eva.v2.database.overlay :refer [empty-overlay batch-advance-overlay]]
            [eva.v2.system.database-catalogue.core :as dbcat]
            [utiliva.sorted-cache :refer [sorted-lru-cache-factory]]))

(defn roots->min-index-position [index-roots]
  (->> index-roots vals (map :tx-num) (apply min)))

(defn index-id [database-id tx-num] [database-id tx-num])

(defprotocol IndexAgent
  (initialize-indexes [ia store database-id base-index-roots target-tx-entry res]
    "Initializes the index agent. Idempotent in that the work to initialize the agent will only happen once.
     Advances the base-index-roots provided to tx-num of target-tx-entry.
     Once advanced delivers the advanced indexes to the promise res as a side effect.")
  (deliver-indexes [ia res]
    "Delivers the indexes contained in the agent to the promise res.
     Throws if the agent has not been initialized."))

(defn index-agent-error-handler [ia ex]
  (log/errorf "Index cache agent failed with: %s"
              (Throwable->map (sanitize ex)))
  nil)

(defrecord IndexAgentImpl [overlaid-indexes]
  IndexAgent
  (initialize-indexes [this store database-id base-overlay target-tx-entry res]
    (if-not (nil? overlaid-indexes)
      (deliver res overlaid-indexes)
      (let [database-info (dbcat/database-info store database-id)
            log (set-log-count (open-transaction-log store database-info) (inc (:tx-num target-tx-entry)))
            overlaid-indexes (batch-advance-overlay base-overlay log target-tx-entry)]
        (deliver res overlaid-indexes)
        (assoc this :overlaid-indexes overlaid-indexes))))
  (deliver-indexes [this res]
    (if (nil? overlaid-indexes)
      (raise ::uninitialized "Cannot deliver indexes from an uninitialized index agent" {})
      (do (deliver res overlaid-indexes)
          this))))

(defprotocol IndexManager
  (retrieve-indexes [im log database-id target-log-entry res]
    "Will deliver the indexes at target-log-entry to the passed promise res.")
  (evict-db [im database-id]
    "Evicts all cached indexes under the given database-id."))

;; global index cache
(defrecord IndexManagerImpl [cache-atom]
  IndexManager
  (retrieve-indexes [this store database-id target-log-entry res]
    (locking this
      (let [target-tx-num (:tx-num target-log-entry)
            basis-roots (:index-roots target-log-entry)
            basis-tx-num (roots->min-index-position basis-roots)
            base-id (index-id database-id basis-tx-num)
            target-id (index-id database-id target-tx-num)
            cache @cache-atom
            croots (rsubseq cache >= base-id <= target-id)
            cached-roots (first croots)]
        (if-not (nil? cached-roots)
          (let [[[_ cached-tx-num :as cached-id] cached-agent] cached-roots]
            (if (= cached-tx-num target-tx-num)
              ;; exact hit
              (do (swap! cache-atom c/hit cached-id)
                  (send-off cached-agent deliver-indexes res))
              ;; proximal hit
              (let [extant-indexes (promise)
                    _ (send-off cached-agent deliver-indexes extant-indexes)
                    new-a (agent (->IndexAgentImpl nil)
                                 :error-handler index-agent-error-handler)]
                (swap! cache-atom c/hit cached-id)
                (swap! cache-atom c/miss target-id new-a)
                (send-off new-a initialize-indexes store database-id @extant-indexes target-log-entry res))))
          ;; miss
          (let [new-a (agent (->IndexAgentImpl nil)
                             :error-handler index-agent-error-handler)]
            (swap! cache-atom c/miss target-id new-a)
            (send-off new-a initialize-indexes store database-id (empty-overlay store basis-roots) target-log-entry res))))))
  (evict-db [this database-id]
    (locking this
      (let [start-purge (index-id database-id Long/MIN_VALUE)
            end-purge   (index-id database-id Long/MAX_VALUE)
            cache       @cache-atom
            purge-range (subseq cache >= start-purge <= end-purge)]
        (swap! cache-atom (fn [cache] (reduce c/evict cache (map key purge-range))))))))

(def global-index-cache
  (let [cache-size (config-strict :eva.v2.storage.index-cache-size)
        cache-atom (atom (sorted-lru-cache-factory {} :threshold cache-size))]
    (->IndexManagerImpl cache-atom)))

(defn get-indexes [store database-id target-log-entry]
  (assert (uuid? database-id))
  (let [res (promise)
        database-id (str database-id)]
    (retrieve-indexes global-index-cache store database-id target-log-entry res)
    @res))

(defn ->evict-callback [database-id]
  (assert (uuid? database-id))
  (fn []
    (log/debug "Evicting cached indexes from manager for database:" database-id)
    (evict-db global-index-cache (str database-id))))
