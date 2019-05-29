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

(ns eva.v2.storage.block-store.impl.memory
  (:require [eva.error :refer [insist]]
            [eva.config :as config]
            [quartermaster.core :as qu]
            [recide.sanex :as sanex]
            [eva.v2.storage.core :refer [BlockStorage]]
            [eva.v2.storage.error :refer [raise-request-cardinality]]
            [eva.v2.storage.block-store.types :as types]
            [clojure.spec.alpha :as s]))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

(s/def ::store-id uuid?)
(s/def ::config
  (s/keys :req [::store-id]))

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;

;;;;;;;;;;
;; IMPL ;;
;;;;;;;;;;

(def ^:private block->key (juxt eva.v2.storage.core/storage-namespace eva.v2.storage.core/storage-id))

(defrecord MemStorage [atom-map max-request-cardinality]
  qu/SharedResource
  (resource-id [this] (some-> (::resource-id this) deref))
  (initiate [this]
    (if (qu/initiated? this)
      this
      (do (reset! atom-map {})
          (assoc this ::resource-id (atom (qu/new-resource-id))))))
  (initiated? [this] (boolean (qu/resource-id this)))
  (status* [this] {})
  (terminate [this]
    (if-not (qu/initiated? this)
      this
      (do (reset! (::resource-id this) nil)
          (reset! atom-map {})
          this)))
  (force-terminate [this] (qu/terminate this))
  BlockStorage
  (storage-read-blocks [this read-mode namespace ids]
    (qu/ensure-initiated! this "cannot read blocks.")
    (when (> (count ids) max-request-cardinality)
      (raise-request-cardinality (format "received %s, limit is %s" (count ids) max-request-cardinality)
                                 {:count (count ids)
                                  :cardinality max-request-cardinality
                                  ::sanex/sanitary? true}))
    (seq (sequence (comp (map (juxt (constantly namespace) identity))
                         (map @atom-map)
                         (filter some?))
                   ids)))
  (storage-write-blocks [this write-mode blocks]
    (qu/ensure-initiated! this "cannot write blocks.")
    (when (> (count blocks) max-request-cardinality)
      (raise-request-cardinality (format "received %s, limit is %s" (count blocks) max-request-cardinality)
                                 {:count (count blocks)
                                  :cardinality max-request-cardinality
                                  ::sanex/sanitary? true}))
    (swap! atom-map
           into
           (zipmap (map block->key blocks)
                   blocks))
    (map #(select-keys % [:namespace :id]) blocks))
  (storage-delete-blocks [this namespace ids]
    (qu/ensure-initiated! this "cannot delete blocks.")
    (when (> (count ids) max-request-cardinality)
      (raise-request-cardinality (format "received %s, limit is %s" (count ids) max-request-cardinality)
                                 {:count (count ids)
                                  :cardinality max-request-cardinality
                                  ::sanex/sanitary? true}))
    (swap! atom-map
           #(transduce (comp (map (juxt (constantly namespace) identity))
                             (map %))
                       dissoc
                       %
                       ids)))
  (storage-compare-and-set-block [this expected replacement]
    (qu/ensure-initiated! this "cannot cas.")
    (let [k-old (block->key expected)
          k-new (block->key replacement)]
      (insist (= k-old k-new))
      (and (= expected (get @atom-map k-old))
           (= replacement
              (get (swap! atom-map
                          #(if (= expected (get % k-old))
                             (assoc % k-old replacement)
                             %))
                   k-new)))))
  (storage-create-block [this block]
    (qu/ensure-initiated! this "cannot create block.")
    (let [k (block->key block)
          after (swap! atom-map
                       #(if (nil? (find % k))
                          (assoc % k block)
                          %))]
      (= block (get after k)))))

(defn mem-storage
  ([] (->MemStorage (atom {}) (config/config-strict :eva.v2.storage.max-request-cardinality)))
  ([max-request-cardinality] (->MemStorage (atom {}) max-request-cardinality)))

;;;;;;;;;;;;
;; SYSTEM ;;
;;;;;;;;;;;;

(def memory-store (memoize (fn [_] (mem-storage))))

(defn build-memory-store
  [{:keys [::store-id]}]
  (memory-store store-id))

(defn memory-store-ident
  [config]
  [::types/memory (select-keys config [::store-id])])
