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

(ns eva.utils.guava-cache
  (:import (clojure.lang Agent)
           (java.util.concurrent ExecutorService TimeUnit)
           (com.google.common.cache CacheLoader CacheBuilder Weigher)
           (com.google.common.util.concurrent SettableFuture)))

(def ^:dynamic ^ExecutorService *executor* Agent/soloExecutor)

(defmacro submit [service & body]
  `(let [f# (fn [] ~@body)]
     (.submit ^ExecutorService ~service ^Callable f#)))

(defn ^CacheLoader cache-loader [load-f load-all-f]
  (let [^ExecutorService exec *executor*]
    (proxy [CacheLoader] []
      (load [k] (load-f k))
      (loadAll [keys]
        (if load-all-f
          (load-all-f keys)
          (proxy-super loadAll keys)))
      (reload [k oldvalue]
        (let [fut (SettableFuture/create)]
          (submit exec (.set fut (load-f k)))
          fut)))))

(defn- ^Weigher fn->weigher [f]
  (reify Weigher
    (weigh [_ k v] (f k v))))

(defn ^CacheBuilder cache-builder
  [{:as opts :keys [concurrency-level
                    expire-after-access
                    expire-after-write
                    refresh-after-write
                    soft-values
                    weak-keys
                    weak-values
                    maximum-size
                    maximum-weight
                    weigher
                    time-unit
                    record-stats]
    :or {^TimeUnit time-unit TimeUnit/MILLISECONDS
         record-stats true}}]
  (let [^CacheBuilder builder (CacheBuilder/newBuilder)]
    (when maximum-size
      (assert (nil? maximum-weight) "maximum-weight and maximum-size cannot be used simultaneously")
      (.maximumSize builder (long maximum-size)))
    (when maximum-weight (.maximumWeight builder (long maximum-weight)))
    (when weigher (.weigher builder (if (fn? weigher) (fn->weigher weigher) weigher)))
    (when concurrency-level (.concurrencyLevel builder (int concurrency-level)))
    (when expire-after-access (.expireAfterAccess builder (long expire-after-access) time-unit))
    (when expire-after-write (.expireAfterWrite builder (long expire-after-write) time-unit))
    (when refresh-after-write (.refreshAfterWrite builder (long refresh-after-write) time-unit))
    (when soft-values (.softValues builder))
    (when weak-keys (.weakKeys builder))
    (when weak-values (.weakValues builder))
    (when record-stats (.recordStats builder))
    builder))

(defn loading-cache
  ([loader-f opts] (loading-cache loader-f nil opts))
  ([loader-f batch-loader-f opts]
   {:pre [(fn? loader-f) (or (nil? batch-loader-f) (fn? batch-loader-f))]}
   (.build (cache-builder opts) (cache-loader loader-f batch-loader-f))))
