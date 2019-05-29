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

(ns eva.v2.storage.value-store.gcached
  (:require [eva.utils.guava-cache :as gcache]
            [eva.v2.storage.error :refer [raise-npe]]
            [recide.sanex :as sanex]
            [recide.sanex.logging :as log]
            [eva.v2.storage.value-store :refer [get-value get-values put-value put-values replace-value get-partition create-key remove-key remove-keys]]
            [eva.v2.storage.value-store.core :as value-store]
            [eva.v2.storage.value-store.concurrent :as conc-vs]
            [eva.v2.database.core :as db]
            [tesserae.core :as tess]
            [barometer.core :as metrics]
            [utiliva.core :refer [map-keys]]
            [eva.v2.utils.spec :refer [conform-spec]]
            [quartermaster.core :as qu]
            [eva.config :refer [config-strict]]
            [clojure.spec.alpha :as s])
  (:import [eva.v2.storage.value_store.protocols IValueStorage ICacheStorage]
           [com.google.common.cache LoadingCache Cache CacheStats]
           [clojure.lang IObj Associative ExceptionInfo PersistentHashSet]
           [com.google.common.util.concurrent UncheckedExecutionException]))

(s/def ::config (s/merge ::value-store/config
                         (s/keys :req [::db/id]
                                 :opt-un [::uncached-keys])))

(def ^:dynamic *cache-opts*
  {:maximum-size (config-strict :eva.v2.storage.value-cache-size)
   :record-stats true
   :concurrency-level 12}) ;; TODO: just testing

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TRACK ALL ACTIVE CACHES ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defonce active-caches (atom #{}))
(defn add-active-cache! [^Cache c] (swap! active-caches conj c))
(defn remove-active-cache! [^Cache c] (swap! active-caches disj c))

;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; GUAVA CACHE LIFECYCLE ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; To change this so that it is no longer a global cache, just change the discriminator used by the manager.
;; When this cache was made global, we saw a horrible no-good terrible blow-up of threads in deployment.
;; They were all from the clojure-agent-send-off-pool. After much investigation, we never quite traced
;; down the cause. After introducing JMS 1.1 code the problem dissapeared, but
;; without the cause being understood, we have every reason to think that this issue could crop
;; up again in the future.

(qu/defmanager guava-cache-manager
  :discriminator (fn [_ _] :global)
  :constructor (fn [_ cache-opts]
                 ;; cache-opts arg should be ignored in favor of global opts if we're using a global singleton
                 (let [new-cache (.build (gcache/cache-builder *cache-opts*))]
                   (add-active-cache! new-cache)
                   new-cache))
  :terminator (fn [cache] (remove-active-cache! cache)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; METRICS FOR ALL CACHES ;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(def x-all-active-caches (keep #(when (instance? Cache %) %)))
(def x-cache-stats (map #(.stats ^Cache %)))
(defn all-cache-stats [caches] (sequence (comp x-all-active-caches x-cache-stats) caches))
(defn combine-cache-stats [caches]
  (when-some [all-stats (not-empty (all-cache-stats caches))]
    (reduce (fn [^CacheStats s1 ^CacheStats s2] (.plus s1 s2)) all-stats)))

(defn get-cache-stat [^CacheStats s k]
  (if-not s
    0
    (case k
      :hit-rate (.hitRate s)
      :hit-count (.hitCount s)
      :miss-rate (.missRate s)
      :miss-count (.missCount s)
      :avg-load-penalty (.averageLoadPenalty s))))

(defn get-cache-sizes []
  (transduce (comp x-all-active-caches
                   (map #(.size ^LoadingCache %)))
             + 0 @active-caches))

(defn get-cache-max-sizes []
  (let [c (count (sequence x-all-active-caches @active-caches))]
    (* c (config-strict :eva.v2.storage.value-cache-size))))

(defn active-cache-stat [k]
  (let [cumulative-stats (combine-cache-stats @active-caches)]
    (get-cache-stat cumulative-stats k)))

(def active-cache-metrics
  (letfn [(mname [s] (str *ns* ".combined." s))]
    {(mname "hit_rate")         (metrics/gauge #(active-cache-stat :hit-rate)
                                               "Combined hit-rate for all caches across all active value-stores")
     (mname "miss_rate")        (metrics/gauge #(active-cache-stat :miss-rate)
                                               "Combined miss-rate for all caches across all active value-stores")
     (mname "approximate_size") (metrics/gauge get-cache-sizes
                                               "Approximate combined size for all caches across all active value-stores")
     (mname "maximum_size")     (metrics/gauge get-cache-max-sizes
                                               "Maximum combined size for all caches across all active value-stores.")}))

(metrics/register-all metrics/DEFAULT active-cache-metrics)

;;;;;;;;;;;;;;;;;;;
;; KEY STRUCTURE ;;
;;;;;;;;;;;;;;;;;;;

(defrecord CacheKey [database-id partition-id storage-key])
(defn gen:->cache-key
  [database-id partition-id]
  (fn ->cache-key
    [storage-key]
    (CacheKey. database-id partition-id storage-key)))

;; ValueStore that caches using a Guava LoadingCache
(defrecord GuavaCachedValueStore [resource-id config value-store ->cache-key cache-opts uncached-keys cache]
  qu/SharedResource
  (resource-id [_] (some-> resource-id deref))
  (initiate [this]
    (if (qu/initiated? this)
      this
      (qu/acquiring [res-id (qu/new-resource-id)
                     value-store (qu/acquire conc-vs/concurrent-value-store-manager res-id config)
                     cache (qu/acquire guava-cache-manager res-id cache-opts)]
        (assoc this
               :cache cache
               :value-store value-store
               :resource-id (atom res-id)))))
  (terminate [this]
    (if (qu/terminated? this)
      this
      (let [res-id @resource-id]
        (reset! resource-id nil)
        (qu/release value-store true)
        (qu/release cache true)
        (assoc this
               :cache nil
               :resource-id nil
               :value-store nil))))
  (force-terminate [this]
    (if (qu/terminated? this)
      this
      (do (qu/force-terminate value-store)
          (qu/reinitiate cache) ;; it's not a SharedResource.
          (qu/terminate this))))
  (initiated? [this] (boolean (qu/resource-id this)))
  (status* [this] {})
  ICacheStorage
  (getIgnoredKeys [this]
    (qu/ensure-initiated! this "ignored keys would be meaningless.")
    @uncached-keys)
  (addIgnoredKey [this ^String k]
    (qu/ensure-initiated! this "adding an ignored key would be pointless.")
    (swap! uncached-keys conj k))
  (removeIgnoredKey [this ^String k]
    (qu/ensure-initiated! this "removing an ignored key is futile.")
    (swap! uncached-keys disj k) true)
  IValueStorage
  (getPartition [this]
    (qu/ensure-initiated! this "cannot get partition.")
    (get-partition @value-store))
  (getValue [this k]
    (qu/ensure-initiated! this "cannot get value.")
    (if (contains? @uncached-keys k)
        (get-value @value-store k)
        (if-let [v (.getIfPresent ^Cache @cache (->cache-key k))]
          (-> (tess/promise) (tess/fulfil v))
          (let [tessera (get-value @value-store k)]
            (tess/chain :annex-delay tessera
                                 (fn [maybe-v]
                                   (when maybe-v
                                     (.put ^Cache @cache (->cache-key k) maybe-v)
                                     maybe-v)))))))
  (getValues [this ks]
    (qu/ensure-initiated! this "cannot get values.")
    (let [uncachable-ks (filter @uncached-keys ks)
          cachable-ks (remove @uncached-keys ks)
          cache-results (->> (map ->cache-key cachable-ks)
                             (.getAllPresent ^Cache @cache)
                             (into {} (map-keys :storage-key)))
          cache-misses (into uncachable-ks
                             (filter (complement (partial contains? cache-results)))
                             cachable-ks)
          future-kvs (get-values @value-store cache-misses)]
      (tess/chain :annex-delay future-kvs
                           (fn [store-results]
                             (let [cache-updates (into {}
                                                       (comp (remove (comp @uncached-keys key))
                                                             (map-keys ->cache-key))
                                                       store-results)]
                               (.putAll ^Cache @cache cache-updates))
                             (merge {} cache-results store-results)))))
  (putValue [this k v]
    (qu/ensure-initiated! this "cannot put value.")
    (if (contains? @uncached-keys k)
        (put-value @value-store k v)
        (let [put (put-value @value-store k v)]
          (tess/chain :annex-delay put
                               (fn [put?]
                                 (when put? (.put ^Cache @cache (->cache-key k) v))
                                 put?)))))
  (putValues [this kvs]
    (qu/ensure-initiated! this "cannot put values.")
    (let [cachable-keys (remove @uncached-keys (keys kvs))
          put-all (put-values @value-store kvs)]
      (tess/chain :annex-delay put-all
                           (fn [put-ks]
                             (.putAll ^Cache @cache
                                      (into {}
                                            (map-keys ->cache-key)
                                            (-> (into {} kvs)
                                                (select-keys put-ks) ;; limit to what was written
                                                (select-keys cachable-keys)))) ;; limit to cachable
                             put-ks))))
  (removeKey [this k]
    (qu/ensure-initiated! this "cannot remove key.")
    (if (contains? @uncached-keys k)
        (remove-key @value-store k)
        (let [tessera (remove-key @value-store k)]
          (tess/chain :annex-delay tessera
                               (fn [removed?]
                                 (when removed?
                                   (.invalidate ^Cache @cache (->cache-key k)))
                                 removed?)))))
  (removeKeys [this ks]
    (qu/ensure-initiated! this "cannot remove keys.")
    (let [cachable-keys (set (remove @uncached-keys (keys ks)))
          tessera (remove-keys value-store ks)]
      (tess/chain :annex-delay tessera
                           (fn [removed-ks]
                             (.invalidateAll ^Cache @cache
                                             (for [[k removed?] removed-ks
                                                   :when (and removed? (cachable-keys k))]
                                               (->cache-key k)))
                             removed-ks))))
  (replaceValue [this k prev curr]
    (qu/ensure-initiated! this "cannot replace value.")
    (if (contains? @uncached-keys k)
        (replace-value @value-store k prev curr)
        (let [tessera (replace-value @value-store k prev curr)]
          (tess/chain :annex-delay tessera
                               (fn [replaced?]
                                 (when replaced?
                                   (.put ^Cache @cache (->cache-key k) curr))
                                 replaced?)))))
  (createKey [this k v]
    (qu/ensure-initiated! this "cannot create key.")
    (if (contains? @uncached-keys k)
      (create-key @value-store k v)
      (let [tessera (create-key @value-store k v)]
        (tess/chain :annex-delay tessera
                             (fn [created?]
                               (when created?
                                 (.put ^Cache @cache (->cache-key k) v))
                               created?))))))

(defn discriminator [_ config]
  {:pre [(conform-spec ::config config)]}
  (::value-store/partition-id config))

(defn constructor [_ config]
  (let [value-cache-size (config-strict :eva.v2.storage.value-cache-size)
        ->cache-key (gen:->cache-key (::db/id config) (::value-store/partition-id config))]
    (map->GuavaCachedValueStore
     {:cache-opts *cache-opts*,
      :uncached-keys (atom #{}),
      :config config,
      :->cache-key ->cache-key})))

(qu/defmanager cached-value-store-manager
  :discriminator discriminator
  :constructor constructor)
