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

(ns eva.server.v2.config-monitor.alpha
  (:import (java.util.concurrent
             Executors
             ScheduledThreadPoolExecutor
             TimeUnit ThreadFactory ScheduledFuture)
           (java.util.concurrent.atomic AtomicInteger)))


(def ^:private ^ThreadFactory pool-thread-factory
  (let [thread-counter (AtomicInteger.)
        thread-group (-> (or (System/getSecurityManager)
                             (Thread/currentThread))
                         (.getThreadGroup))
        thread-name-prefix (str *ns* "/pool-thread-")]
    (reify ThreadFactory
      (newThread [_ runnable]
        (let [t (Thread. thread-group
                         runnable
                         (str thread-name-prefix (.getAndIncrement thread-counter)))]
          ;; config-monitor threads should NOT block jvm exit
          (.setDaemon t true)
          (when (not= (.getPriority t) Thread/NORM_PRIORITY)
            (.setPriority t Thread/NORM_PRIORITY))
          t)))))

(defonce scheduled-pool (delay (Executors/newScheduledThreadPool 1 pool-thread-factory)))

(defn check! [changed? prev-val get-curr-val on-change on-error]
  (try
    (let [curr-val (get-curr-val)]
      (when (changed? prev-val curr-val)
        (on-change prev-val curr-val)))
    (catch Throwable e
      (on-error e))))

(defn monitor!
  ^ScheduledFuture [period-ms changed? prev-val get-curr-val on-change on-error]
  (let [^ScheduledThreadPoolExecutor s @scheduled-pool]
    (.scheduleAtFixedRate s
                          #(check! changed? prev-val get-curr-val on-change on-error)
                          period-ms
                          period-ms
                          TimeUnit/MILLISECONDS)))


(comment

  (def state (atom 0))

  (def sf (monitor! 1000
                    not=
                    @state
                    #(deref state)
                    (fn [prev curr]
                      (println "changed from" prev "to" curr))
                    (fn [err]
                      (println "error occurred:" err))))

  (swap! state inc)
  (.cancel sf true)
  )
