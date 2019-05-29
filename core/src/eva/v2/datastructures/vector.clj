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

(ns eva.v2.datastructures.vector
  "Implementation of an append-only immutable vector in secondary storage.
  This implementation should behave correctly and safely under concurrent
  attempts to read and write. Writing to the end of the vector and reading from
  any position in the vector should be O(1). This data structure is intended to
  support the immutable write-ahead transaction log backing an Eva database."
  (:require [eva.v2.storage.value-store :as vs]
            [manifold.deferred :as d]
            [eva.config :refer [config-strict]]
            [eva.datastructures.protocols :as dsp]
            [eva.datastructures.error :as derr]
            [eva.v2.datastructures.var :refer [->PersistedVar]]
            [eva.v2.datastructures.atom :refer [->PersistedAtom]]
            [eva.error :refer [raise]]
            [recide.sanex :as sanex]
            [recide.sanex.logging :as log]
            [utiliva.uuid :refer [squuid]]
            [ichnaie.core :refer [tracing traced]]
            [morphe.core :as defm])
  (:import [eva.v2.storage.value_store.protocols IValueStorage]))

(defn init-head [] {:count 0})
(defn next-head [cur-head] (update cur-head :count inc))

(defn nth-key [head-key n] (str head-key "." n))

(deftype PersistedVector [^IValueStorage
                          store
                          head-key
                          cur-head
                          _meta]
  dsp/BackedStructure
  (storage-id [_] head-key)
  (store [_] store)
  Object
  (equals [this obj]
    (and (instance? PersistedVector obj)
         (= @store @(.-store ^PersistedVector obj))
         (= head-key (.-head-key ^PersistedVector obj))))
  clojure.lang.IPersistentVector
  (cons [this obj]
    (tracing "PersistentVector.cons"
             (let [created? (tracing "PersistentVector.createEntry"
                                     @(vs/create-key @store (nth-key head-key (count this)) obj))
                   swapped? (tracing "PersistentVector.swapHead"
                                     (if (true? created?)
                                       @(vs/replace-value @store head-key cur-head (next-head cur-head))
                                       (do
                                         (log/warn "create-key failure in persisted vector")
                                         (derr/raise-stale "create-key failure in persisted vector"
                                                           {:method 'cons, ::sanex/sanitary? true}))))]
               (if (true? swapped?)
                 (PersistedVector. store head-key (next-head cur-head) _meta)
                 (let [cur-val @(vs/get-value @store head-key)]
                   (if (nil? cur-val)
                     ;; TODO: If we deal with deletion, we'll need to revisit this.
                     (derr/raise-non-monotonicity "persisted vector no longer exists"
                                                  {:key head-key
                                                   ::sanex/sanitary? true})
                     (do
                       ;; At this point, we know:
                       ;; 1) We have persisted our state successfully
                       ;; 2) Some other process has come in and mucked with the head reference
                       ;;
                       ;; Monotonicity of persisted values is assumed, so as long as the
                       ;; head we got back is newer than the head we had, we can
                       ;; return successfully using the new head we just got
                       ;; from storage.
                       (if (> (:count cur-val) (:count cur-head)) ;; check for weirdness
                         (do
                           (log/info (format "the persisted state for the vector head was found to be more recent than the expected state, returning a new persisted vector using the persisted state."))
                           (PersistedVector. store head-key cur-val _meta))
                         ;; We get here only if our monotonicity assumption is violated.
                         ;; We don't really have a recovery path from here.
                         (do (log/warn (format "persisted vector failed to swap head: expected head %s, found head %s"
                                               cur-head
                                               cur-val))
                             (derr/raise-non-monotonicity (format "persisted vector failed to swap head: expected head %s, found head %s."
                                                                  cur-head
                                                                  cur-val)
                                                          {:head-key head-key
                                                           :cur-head cur-head
                                                           ;; Revisit:
                                                           ::sanex/sanitary? true}))))))))))
  (equiv [this obj] (.equals this obj))
  clojure.lang.IndexedSeq
  (count [_] (:count cur-head))
  (first [this] (nth this 0))
  (more  [this] (map (partial nth this) (range 1 (count this))))
  (next [this] (seq (rest this)))
  (seq [this] (map (partial nth this) (range (count this))))
  (nth [this i]
    (if (and (<= 0 i (count this)) (not= 0 (count this)))
      (->PersistedVar store (nth-key head-key i))
      (throw (IndexOutOfBoundsException. (str "cannot retrieve index " i ". Vector has count " (count this))))))
  clojure.lang.IObj
  (withMeta [this meta] (PersistedVector. store head-key cur-head meta))
  clojure.lang.IMeta
  (meta [this] _meta))

(defmethod print-method PersistedVector
  [v ^java.io.Writer w]
  (.write w (str "PersistedVector:" (count v))))

(defn ^PersistedVector create-persisted-vector [value-store]
  (let [head-key (str (squuid))
        init-head-value (init-head)]
    @(vs/create-key @value-store head-key init-head-value)
    (vs/add-ignored-key! @value-store head-key)
    (PersistedVector. value-store head-key init-head-value {})))

(defn ^PersistedVector open-persisted-vector [value-store head-key]
  (let [head-value @(vs/get-value @value-store head-key)]
    (if (some? head-value)
      (do (vs/add-ignored-key! @value-store head-key)
          (PersistedVector. value-store head-key head-value {}))
      (raise :datastructures/no-such-vector
             (format "Cannot find a persisted vector for key %s" head-key)
             {:head-key head-key
              ::sanex/sanitary? true}))))

;; TODO: spec head-value
(defn ^PersistedVector set-cur-head-value [pv head-value]
  (let [new-count (max (count pv) (:count head-value))]
    (PersistedVector. (dsp/store pv) (dsp/storage-id pv) {:count new-count} {})))

(defn flip [f x y] (f y x))

(defn chunked-map
  "Maps f over coll lazily in chunks of size batch-size"
  [f batch-size coll]
  (let [[cur later] (split-at batch-size coll)]
    (if (empty? cur)
      (list)
      (lazy-seq
       (reduce (partial flip cons)
               (chunked-map f batch-size later)
               (reverse (f cur)))))))

(defn realize-chunk [store vs]
  (let [ks (map :k vs)]
    (map @(vs/get-values @store ks) ks)))

(defn lazy-read-range
  "Realizes elements [x..y], exclusive on y, from the persisted vector
   lazily.  Will realize in chunks of size `batch-size' and asynchronously
   will attempt to stay `read-ahead' chunks ahead of the last-read chunk"
  ([^PersistedVector v x y]
   (lazy-read-range v x y
                    (config-strict :eva.log.read-chunk-size)
                    (config-strict :eva.log.read-chunks-ahead)))
  ([^PersistedVector v x y batch-size read-ahead]
   (let [store (.store v)]
     (seque read-ahead
            (chunked-map (partial realize-chunk store)
                         batch-size
                         (subvec v x y))))))

(defn read-range
  "Realizes elements [x..y], exclusive on y,from the persisted vector in batch"
  [^PersistedVector v x y]
  (if (not= x y)
    (let [sv (subvec v x y)
          ks (map :k sv)
          store (.store v)]
      (d/chain (vs/get-values @store ks)
               (fn [res-map] (map res-map ks))))
    (delay [])))

(prefer-method clojure.pprint/simple-dispatch clojure.lang.IPersistentVector clojure.lang.ISeq)

(defm/defn ^{::defm/aspects [traced]} repair-vector-head
  "Given the current vector cons, it's possible for a new key / entry to be
   created but the cas process on the head fails afterwards, leaving the
   vector in an unusable state, where the head is one value behind the true
   length of the vector. This function will attempt to repair that state.

   We do the following:

   1. Refresh the persisted vector to head state.

   2. Confirm that the key beyond the end of the head state does exist.
      --> someone has repaired in the meantime, warn that this happened,
          return the refreshed vector.

   3. Attempt to cas the head state manually.
      --> cas fails : throw an exception, concurrent attempts to repair.

   4. Return the rebuilt vector."
  [^PersistedVector pv store]
  (let [storage-id (dsp/storage-id pv)
        refreshed-vector (open-persisted-vector store storage-id)
        length (count refreshed-vector)
        key-of-interest (nth-key storage-id length)
        val-of-interest @(vs/get-value @store key-of-interest)]
    (if (nil? val-of-interest)
      (do (log/warn "Ignoring call to repair undamaged persisted vector. Returning most recent known vector.")
          refreshed-vector)
      (do
        (log/debug "Call to repair found damaged vector, attempting repair.")
        (let [cur-head (.cur-head ^PersistedVector refreshed-vector)
              _ (log/warn "repairing:" cur-head (next-head cur-head))
              repaired-head @(vs/replace-value @store storage-id cur-head (next-head cur-head))]
          (if (true? repaired-head)
            (PersistedVector. store storage-id (next-head cur-head) {})
            (derr/raise-concurrent "concurrent attempt to repair vector detected."
                                   {::sanex/sanitary? true})))))))
