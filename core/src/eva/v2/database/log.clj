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

(ns eva.v2.database.log
  (:require [eva.core :as core :refer [Log LogEntry entry->datoms initialize-index ->root packed-datoms]]
            [eva.utils :refer [init-log-info]]
            [eva.v2.datastructures.vector :as ds2v]
            [eva.bytes :refer [bbyte-array?]]
            [eva.datom :as dat]
            [eva.utils :refer [init-log-info with-retries]]
            [eva.defaults :as defaults]
            [eva.error :refer [insist with-api-error-handling]]
            [eva.v2.storage.value-store :refer [get-values]]
            [eva.entity-id :as entity-id]
            [schema.core :as schema] ;; TODO: deprecate schema.core
            [clojure.spec.alpha :as s])
  (:import [eva.v2.datastructures.vector PersistedVector]))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

(s/def ::version any?) ;; TODO: specify format for log version.

(s/def ::head
  (s/keys :req-un [:list/count
                   :list/current-key]))

(s/def ::versioned-entry
  (s/and (partial satisfies? LogEntry)
         (s/keys :req-un [::version])))

(s/def ::entry ::versioned-entry)

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;

(def LOG_ENTRY_VERSION 1)

(schema/defrecord TransactionLogEntry
    [index-roots
     cur-tx-eid ;; The current transaction entity id; increments by 1 with every transaction
     cur-max-id ;; The current maximum id sans partition; increments by 1 per eid allocated.
     tx-num     ;; The index of this log entry; increments by 1 with every transaction
     tx-inst
     ref-type-attrs ;; The set of reference-type attributes in this entry
     novelty  ;; [[packed-eid a v]]
     version]
  LogEntry
  (entry->datoms [this]
    (entry->datoms this :eavt))
  (entry->datoms [_ index]
    (map (partial dat/unpack-datom cur-tx-eid) novelty))
  (packed-datoms [_] (map #(conj % cur-tx-eid ) novelty))
  (packed-ref-type-datoms [this]
    (filter (comp (partial contains? ref-type-attrs) second) (packed-datoms this)))
  (packed-non-byte-datoms [this]
    (remove (comp bbyte-array? #(nth % 2)) (packed-datoms this))))

(defn valid-log-index? [x]
  (or (nil? x)
      (entity-id/tx-eid? x)
      (entity-id/tx-num? x)))

(defrecord PersistedVectorLog [^PersistedVector pv]
  eva.Log
  (txRange [_ start-t end-t]
    (with-api-error-handling
      (insist (valid-log-index? start-t))
      (insist (valid-log-index? end-t))
      (let [start-t (if (nil? start-t)
                      0
                      (-> start-t entity-id/->tx-num))
            end-t (if (nil? end-t)
                    (count pv)
                    (-> end-t entity-id/->tx-num))
            store (.store pv)]
        (insist (< start-t end-t) "tx-range: start-t must be less than end-t")
        (sequence
         (comp (map #(nth pv %))
               (map :k)
               (partition-all 5)
               (mapcat (fn fetch-log-entries [ks]
                         (with-retries defaults/read-log-range-retry-strategy
                           (format "error fetching log entries, retrying: %s" (pr-str ks))
                           (let [values @(get-values @store ks)]
                             (for [k ks] (get values k))))))
               (map (fn [log-entry]
                      {:pre [(satisfies? LogEntry log-entry)
                             (some? (:tx-num log-entry))]}
                      {:t    (:tx-num log-entry)
                       :data (entry->datoms log-entry)})))
         (range (max start-t 0)
                (min end-t (count pv))))))))

(defn set-log-count
  "Assuming an independent update to the log, explicitly set the local
   length to the max of the current value and given value."
  [v count]
  (ds2v/set-cur-head-value v {:count count}))

(defn log-entry
  "Retrieves the log entry at the given tx-num, or the most recent if no num
   is provided"
  ([log] (log-entry log (dec (count log))))
  ([log tx-num] (nth log tx-num)))

(declare build-init-log)

(defn open-transaction-log
  [store {:as database-info :keys [tx-log-id]}]
  (ds2v/open-persisted-vector store tx-log-id))
