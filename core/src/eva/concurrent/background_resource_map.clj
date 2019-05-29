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

(ns eva.concurrent.background-resource-map
  "This is a possibly temporary solution to the background realization of delayed
  resources. Each background-resource-map uses the same global (currently unbounded)
  thread pool, but aims for utilizing only two threads at a time to realize resources
  in order of priority as determined by an arbitrary selector function passed in."
  (:require [eva.utils.delay-queue :as d-q]
            [eva.config :refer [config-strict]]
            [recide.sanex.logging :refer [warn]])
  (:import [java.util.concurrent TimeUnit
            ThreadPoolExecutor
            ThreadPoolExecutor$DiscardOldestPolicy
            ThreadFactory
            ArrayBlockingQueue
            ExecutorService]))

(defonce thread-counter (atom -1))

;; http://stackoverflow.com/a/1800583/2472391
(defonce global-background-resource-pool
  (ThreadPoolExecutor. 8 ;; core thread size
                       (config-strict :eva.concurrent.background-resource.max)
                       120 ;; seconds vvvv
                       TimeUnit/SECONDS
                       (ArrayBlockingQueue. (config-strict :eva.concurrent.background-resource.queue-size))
                       (reify ThreadFactory
                         (newThread [_ runnable]
                           (doto (Thread. runnable)
                             (.setName (format "background-resource-map-%d" (swap! thread-counter inc))))))
                       (ThreadPoolExecutor$DiscardOldestPolicy.)))

(defn get-next!
  "Like a swap!, but doesn't return the new value of the atom.
  Instead returns the thing selected and dissoced from the map."
  [select-fn tag->delay]
  (loop []
    (let [snap @tag->delay]
      (when-not (empty? snap)
        (let [entry (select-fn snap)
              new-tag->delay (dissoc snap (key entry))]
          (if (compare-and-set! tag->delay snap new-tag->delay)
            entry
            (recur)))))))

(defrecord TaggedPriorityResourceMap
    [tag->delay select-fn thread-count])

(defn realize-next-resource
  "Tries to select a new delay to realize. If it realizes one, it returns true; otherwise nil."
  [resource-map]
  (let [[tag delay] (get-next! (:select-fn resource-map) (:tag->delay resource-map))]
    (when (some? delay)
      (deref delay)
      true)))

(defn thread-flow
  "Try to realize-next-resource; if that returns true, repeat; otherwise, exit.
  In the event that an exception is caught, it logs it at warn-level and exits."
  [resource-map]
  (when (try
          (when-not (.isInterrupted (Thread/currentThread))
            (realize-next-resource resource-map))
          (catch Exception e
            (warn e "Background resource processor encountered an error. Logging.")
            false))
    (recur resource-map)))

(defn enqueue-background-update
  "Essentially a wrapper over delay-queue/enqueue-update to enable the magic."
  [resource-map dq tag f & args]
  (let [dq (d-q/enqueue-f dq f args)
        tag->delay (swap! (:tag->delay resource-map)
                          (fn [tag->delay]
                            (assoc tag->delay tag dq)))]
    (when (= 1 (count tag->delay))
      (doseq [task (repeat (:thread-count resource-map) (fn [] (thread-flow resource-map)))]
        (.submit ^ExecutorService global-background-resource-pool ^java.lang.Runnable task)))
    dq))

(defn background-resource-map
  "select-fn is expected to take a map of tag->delay and return the MapEntry chosen
  to be realized next."
  [select-fn]
  (let [tag->delay (atom {})
        thread-count (config-strict :eva.concurrent.background-resource.thread-goal)
        resource-map (->TaggedPriorityResourceMap tag->delay
                                                  select-fn
                                                  thread-count)]
    resource-map))
