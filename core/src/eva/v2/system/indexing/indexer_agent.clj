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

(ns eva.v2.system.indexing.indexer-agent
  (:require [eva.v2.database.log :refer [open-transaction-log set-log-count]]
            [eva.config :refer [config-strict]]
            [eva.core :refer [batch-advance-index ->root]]
            [eva.v2.database.index :refer [flush-index]]
            [recide.sanex :refer [sanitize]]
            [recide.sanex.logging :as log]
            [eva.v2.datastructures.bbtree :refer [persist! in-mem-nodes]]
            [eva.sizing-api :as sapi]))

(defprotocol IndexerAgent
  (tx-delta [this] "The number of staged transactions applied to this index")
  (pending [this res] "Requests that the pending updates for this agent be delivered to res.")
  (flush! [this] "Flushes the staged transaction updates, creates pending.")
  (advance-index [this tx-log-entry] "Update the staging index to include everything up to tx-log-entry. Clears pending."))

(defrecord IndexerAgentImpl [database-id tx-log _basis-tx _cur-tx staging-index pending]
  IndexerAgent
  (tx-delta [this] (- (:tx staging-index) _basis-tx))
  (pending [this res] (deliver res pending))
  (advance-index [this tx-log-entry]
    (let [entry-num (:tx-num tx-log-entry)]
      (if (> entry-num _cur-tx)
        (do (-> (update this :staging-index
                        batch-advance-index
                        (set-log-count tx-log (inc (inc entry-num)))
                        tx-log-entry)
                (assoc :_cur-tx entry-num)
                (assoc :pending nil)))
        (do (log/warnf "Ignoring call to advance %s in %s from tx %s to %s"
                       (:name staging-index)
                       database-id
                       _cur-tx
                       entry-num)
            this))))
  (flush! [this]
    (let [flushed-index (update staging-index :idx persist!)]
      (assoc this
             :staging-index flushed-index
             :_basis-tx _cur-tx
             :pending (->root flushed-index))))
  sapi/SizeEstimable
  (ram-size [this]
    ;; TODO: Investigate whether this is a good enough estimate of size.
    (let [num-unpersisted-nodes (count (in-mem-nodes (:idx staging-index)))]
      (* num-unpersisted-nodes
         (config-strict :eva.v2.storage.block-size)))))

(defn indexer-agent-error-handler [ia ex]
  (log/errorf "Indexer agent for %s in %s failed with: %s"
              (-> ia deref :staging-index :name)
              (-> ia deref :database-id)
              (Throwable->map (sanitize ex)))
  nil)

(defn indexer-agent [database-id tx-log index]
  (let [basis-tx (:tx index)]
    (agent (->IndexerAgentImpl database-id tx-log basis-tx basis-tx index nil)
           :error-handler indexer-agent-error-handler)))
