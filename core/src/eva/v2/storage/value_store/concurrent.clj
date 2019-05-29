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

(ns eva.v2.storage.value-store.concurrent
  (:require [eva.v2.storage.value-store.functions :as f]
            [eva.v2.storage.value-store.core :as vs-core]
            [eva.v2.storage.block-store :as bs]
            [eva.v2.storage.block-store.types :as bs-types]
            [eva.v2.storage.value-store.protocols :refer :all]
            [eva.v2.storage.core :refer :all]
            [flowgraph.core :refer [deflow]]
            [flowgraph.edge :refer [transform discriminate fuse duplicate]]
            [flowgraph.protocols :refer [submit shutdown]]
            [tesserae.core :as tess]
            [barometer.core :as em]
            [eva.v2.storage.error :refer [raise-conc-err raise-npe]]
            [recide.sanex :as sanex]
            [quartermaster.core :as qu]
            [eva.v2.utils.spec :refer [conform-spec]]
            [eva.error :refer [insist error?]]
            [recide.core :refer [try*]]
            [eva.config :refer [config-strict]]
            [eva.v2.fressian :refer [eva-write-handlers eva-read-handlers]])
  (:import [java.io Closeable]
           [eva.v2.storage.value_store.protocols IValueStorage])
  (:refer-clojure :exclude [partition merge]))

;; ======================================================================
;; GLOBAL COMPUTATION GRAPHS
;; ======================================================================

(deflow reader-graph
  [handlers]
  {:source (transform f/fetch-blocks :fetched
                      :asynchronous? true
                      :timeout-ms (config-strict :eva.v2.storage.request-timeout-ms)
                      :batching? (config-strict :eva.v2.storage.max-request-cardinality))
   :fetched (discriminate f/sharded? {true :shard-heads, false :ashards})
   :ashards (transform (comp (f/deserialize-gen handlers)
                             f/unzip
                             f/ensure-bytebuffer
                             f/decrypt)
                       :sink)
   :shard-heads (transform (comp (f/deserialize-gen handlers)
                                 f/unzip
                                 f/ensure-bytebuffer
                                 f/decrypt)
                           :deserialized-shards
                           :priority 2)
   :deserialized-shards (duplicate [:shard-shell :shard-innards] :coordinated? true)
   :shard-innards (transform f/seq-and-tag-shard-addresses :shard-addresses
                             :batching? 1
                             :priority 2
                             :coordinated? true)
   :shard-addresses (transform f/fetch-blocks :shard-pieces
                               :batching? (config-strict :eva.v2.storage.max-request-cardinality)
                               :priority 2
                               :asynchronous? true
                               :timeout-ms (config-strict :eva.v2.storage.request-timeout-ms)
                               :coordinated? true)
   :shard-pieces (transform f/unshard :unsharded-innards
                            :collecting? f/completes-shard
                            :collect-inclusive? true
                            :collect-strict? true
                            :coordinated? true
                            :priority 2)
   [:shard-shell :unsharded-innards] (fuse :recombined-shards :coordinated? true)
   :recombined-shards (transform f/wrap-shell-round-innards :fetched :priority 2)}
  {:source {:priority-queue? f/sharded-comparator}})

(deflow writer-graph
  [max-size handlers]
  {:source (transform (comp f/zip (f/serialize-gen handlers)) :serialized)
   :serialized (discriminate (f/too-large? max-size) {true :to-be-sharded, false :to-be-encrypted})
   :to-be-sharded (transform (comp (f/shard max-size) first) :maybe-serialized :batching? 1)
   :maybe-serialized (discriminate f/sharded? {true :source, false :to-be-encrypted})
   :to-be-encrypted (transform (comp f/encrypt f/bytebuffer->byte-string) :to-be-written)
   :to-be-written (transform f/push-blocks :sink
                             :asynchronous? true
                             :timeout-ms (config-strict :eva.v2.storage.request-timeout-ms)
                             :batching? (config-strict :eva.v2.storage.max-request-cardinality))})

(qu/defmanager reader-graph-manager
  :discriminator
  (fn [_ _] :global)
  :constructor
  (fn [_ _] (reader-graph eva-read-handlers))
  :terminator
  (fn [graph] (shutdown graph)))

(qu/defmanager writer-graph-manager
  :discriminator
  (fn [_ _] :global)
  :constructor
  (fn [_ _] (writer-graph (config-strict :eva.v2.storage.block-size) eva-write-handlers))
  :terminator
  (fn [graph] (shutdown graph)))

;; ======================================================================
;; VALUE STORE
;; ======================================================================

(def get-value-timer (em/timer "Concurrent value store get-value calls."))
(def get-values-timer (em/timer "Concurrent value store get-values calls."))
(def put-value-timer (em/timer "Concurrent value store put-value calls."))
(def put-values-timer (em/timer "Concurrent value store put-values calls."))
(def remove-key-timer (em/timer "Concurrent value store remove-key calls."))
(def remove-keys-timer (em/timer "Concurrent value store remove-keys calls."))
(def replace-value-timer (em/timer "Concurrent value store replace-value calls."))
(em/register-all
 em/DEFAULT
 {'eva.v2.storage.value-store.concurrent.get-value.timer get-value-timer
  'eva.v2.storage.value-store.concurrent.get-values.timer get-values-timer
  'eva.v2.storage.value-store.concurrent.put-value.timer put-value-timer
  'eva.v2.storage.value-store.concurrent.put-values.timer put-values-timer
  'eva.v2.storage.value-store.concurrent.remove-key.timer remove-key-timer
  'eva.v2.storage.value-store.concurrent.remove-keys.timer remove-keys-timer
  'eva.v2.storage.value-store.concurrent.replace-value.timer replace-value-timer})

(defrecord ConcurrentValueStore
    [resource-id config partition block-store reader-graph writer-graph]
  qu/SharedResource
  (resource-id [_] (some-> resource-id deref))
  (initiate [this]
    (if (qu/initiated? this)
      this
      (qu/acquiring [res-id (qu/new-resource-id)
                     block-store (qu/acquire bs/block-store-manager res-id config)
                     reader-graph (qu/acquire reader-graph-manager res-id config)
                     writer-graph (qu/acquire writer-graph-manager res-id config)]
                    (assoc this
                           :resource-id (atom res-id)
                           :block-store block-store
                           :reader-graph reader-graph
                           :writer-graph writer-graph))))
  (initiated? [this] (boolean (qu/resource-id this)))
  (status* [this] {})
  (terminate [this]
    (if (qu/terminated? this)
      this
      (let [res-id @resource-id]
        (reset! resource-id nil)
        (qu/release block-store true)
        (qu/release reader-graph true)
        (qu/release writer-graph true)
        (assoc this
               :cached-block-store nil
               :reader-graph nil
               :writer-graph nil))))
  (force-terminate [this]
    (if (qu/terminated? this)
      this
      (do (qu/force-terminate block-store)
          (qu/force-terminate reader-graph)
          (qu/force-terminate writer-graph)
          (qu/terminate this))))
  Closeable
  (close [this] (qu/terminate this))
  IValueStorage
  (getPartition [_] partition)
  (getValue [this k]
    (qu/ensure-initiated! this "cannot getValue")
    (try* (let [return (submit @reader-graph [(f/->StorageBlob @block-store partition k {} nil)])
                post-process (fn [blobs]
                               (em/with-timer get-value-timer
                                 (try* (let [k->v (zipmap (map :id blobs)
                                                          (map :val blobs))]
                                         (get k->v k))
                                       (catch (:not error?) e
                                         (raise-conc-err :unknown "Exception thrown in get-value"
                                                         {:method 'get-value, :k k, ::sanex/sanitary? false}
                                                         e)))))]
            (tess/chain :annex-delay return post-process))
          (catch (:not error?) e
            (raise-conc-err :unknown "Exception thrown in get-value"
                            {:method 'get-value, :k k, ::sanex/sanitary? false} e))))
  (getValues [this ks]
    (qu/ensure-initiated! this "cannot getValues")
    (try* (let [return (submit @reader-graph (map #(f/->StorageBlob @block-store partition % {} nil) ks))
                post-process (fn [blobs]
                               (em/with-timer get-values-timer
                                 (try* (let [k->v (zipmap (map :id blobs)
                                                          (map :val blobs))]
                                         (zipmap ks (map k->v ks)))
                                       (catch (:not error?) e
                                         (raise-conc-err :unknown "Exception thrown in get-values"
                                                         {:method 'get-values,
                                                          :ks ks,
                                                          ::sanex/sanitary? false}
                                                         e)))))]
            (tess/chain :annex-delay return post-process))
          (catch (:not error?) e
            (raise-conc-err :unknown "Exception thrown in get-values"
                            {:method 'get-values, :ks ks, ::sanex/sanitary? false} e))))
  (createKey [this k v]
    (when (nil? v) (raise-npe "create-key was passed nil for value." {:method 'create-key, ::sanex/sanitary? true}))
    (when (nil? k) (raise-npe "create-key was passed nil for key." {:method 'create-key, ::sanex/sanitary? true}))
    (qu/ensure-initiated! this "cannot createKey")
    (tess/future
      (try*
       (let [source->serialized (comp f/zip (f/serialize-gen eva-write-handlers))
             too-large? (f/too-large? (config-strict :eva.v2.storage.block-size))
             shard (f/shard (config-strict :eva.v2.storage.block-size))
             encrypt (comp f/encrypt f/bytebuffer->byte-string)
             write-all-blobs (fn write-all-blobs [blobs]
                               (every? (comp (into #{}
                                                   (map :id)
                                                   (f/push-blocks (map encrypt blobs)))
                                             :id)
                                       blobs))]
         (loop [blob (source->serialized (f/->StorageBlob @block-store partition k {} v))
                dependencies ()]
           (if (too-large? blob)
             (let [{[head] true shards false}
                   (group-by f/sharded? (shard blob))]
               (recur (source->serialized head) (into dependencies shards)))
             (if (or (empty? dependencies)
                     (write-all-blobs dependencies))
               (create-block @block-store (-> blob encrypt f/blob->block))
               false))))
       (catch (:not error?) e
         (raise-conc-err :unknown "Exception thrown in create-key"
                         {:method 'create-key, :k k, ::sanex/sanitary? false} e)))))
  (putValue [this k v]
    (when (nil? v)
      (raise-npe "put-value was passed nil."
                 {:method 'put-value, :k k, ::sanex/sanitary? true}))
    (qu/ensure-initiated! this "cannot putValue.")
    (try* (let [return (submit @writer-graph (map #(f/->StorageBlob @block-store partition (key %) {} (val %))
                                                  {k v}))
                post-process (fn [blobs]
                               (em/with-timer put-value-timer
                                 (try* (let [ks (into #{} (map :id) blobs)]
                                         (contains? ks k))
                                       (catch (:not error?) e
                                         (raise-conc-err :unknown "Exception thrown in put-value"
                                                         {:method 'put-value, :k k, ::sanex/sanitary? false}
                                                         e)))))]
            (tess/chain :annex-delay return post-process))
          (catch (:not error?) e
            (raise-conc-err :unknown "Exception thrown in put-value"
                            {:method 'put-value :k k, ::sanex/sanitary? false} e))))
  (putValues [this kvs]
    (when (some nil? (vals kvs))
      (raise-npe "put-values was passed nil(s)."
                 {:method 'put-values,
                  :offending-ks (keys (filter (comp nil? val) kvs)),
                  ::sanex/sanitary? true}))
    (qu/ensure-initiated! this "cannot putValues.")
    (try* (let [return (submit @writer-graph (map #(f/->StorageBlob @block-store partition (key %) {} (val %)) kvs))
                post-process (fn [blobs]
                               (em/with-timer put-values-timer
                                 (try*
                                  (let [ks (into #{} (map :id blobs))
                                        keys-of-interest (keys kvs)]
                                    (zipmap keys-of-interest
                                            (map #(contains? ks %)
                                                 keys-of-interest)))
                                  (catch (:not error?) e
                                    (raise-conc-err :unknown "Exception thrown in put-values"
                                                    {:method 'put-values,
                                                     :ks (keys kvs),
                                                     ::sanex/sanitary? false}
                                                    e)))))]
            (tess/chain :annex-delay return post-process))
          (catch (:not error?) e
            (raise-conc-err :unknown "Exception thrown in put-values"
                            {:method 'put-values :ks (keys kvs), ::sanex/sanitary? false} e))))
  (removeKey [this k]
    (qu/ensure-initiated! this "cannot removeKey.")
    (tess/future (em/with-timer remove-key-timer
                   (try* (->> (delete-block @block-store partition k)
                              (= k))
                         (catch (:not error?) e
                           (raise-conc-err :unknown "Exception thrown in remove-key"
                                           {:method 'remove-key, :k k, ::sanex/sanitary? false}))))))
  (removeKeys [this ks]
    (qu/ensure-initiated! this "cannot removeKeys.")
    (tess/future (em/with-timer remove-keys-timer
                   (try* (->> (delete-blocks @block-store partition ks)
                              (reduce #(assoc % %2 true)
                                      (zipmap ks (repeat false))))
                         (catch (:not error?) e
                           (raise-conc-err :unknown "Exception thrown in remove-keys"
                                           {:method 'remove-keys, :ks ks, ::sanex/sanitary? false}))))))
  (replaceValue [this k prev new]
    (when (nil? new) (raise-npe "replace-value was passed nil." {:method 'replace-value, :k k, ::sanex/sanitary? true}))
    (qu/ensure-initiated! this "cannot replaceValue.")
    (tess/future
      (em/with-timer replace-value-timer
        (try* (let [make-block (fn [k v]
                                 (let [blob (-> (f/->StorageBlob nil partition k {} v)
                                                ((f/serialize-gen eva-write-handlers))
                                                (f/zip)
                                                f/bytebuffer->byte-string
                                                f/encrypt)]
                                   (insist (not ((f/too-large? (config-strict :eva.v2.storage.block-size)) blob)))
                                   (f/blob->block blob)))
                    prev-block (make-block k prev)
                    new-block (make-block k new)]
                (boolean (compare-and-set-block @block-store
                                                prev-block
                                                new-block)))
              (catch (:not error?) e
                (raise-conc-err :unknown "Exception thrown in replace-value."
                                {:method 'replace-value, :k k, ::sanex/sanitary? false}
                                e)))))))

(defn discriminator [_ config]
  {:pre [(conform-spec ::vs-core/config config)]}
  [(::vs-core/partition-id config) (bs-types/block-store-ident config)])

(defn constructor [ident config]
  (map->ConcurrentValueStore {:config config,
                              :partition (::vs-core/partition-id config)}))

(qu/defmanager concurrent-value-store-manager
  :discriminator discriminator
  :constructor constructor)
