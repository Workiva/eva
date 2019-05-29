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

(ns eva.utils.delay-queue
  (:require [eva.error :refer [insist]]
            [manifold.deferred :as d]
            [clojure.core.cache :as cache
             :refer [fifo-cache-factory]])
  (:refer-clojure :exclude [memoize]))

(defprotocol DelayedQueue
  (enqueue-f [dq f args])
  (viz [dq]))
(defn enqueue-update [dq f & args] (enqueue-f dq f args))

(defn- realize-step
  [v [p op]]
  (if (realized? p)
    @p
    (locking p
      (if (realized? p)
        @p
        @(deliver p (apply (:fn op) v (:args op)))))))

;; A type of queue in which elements can only be dequeued once it has
;; remained in the queue for a certain length of time.  It goes without saying:
;; the head of the queue has always been in the queue the longest.
(deftype DelayQueue
         [^clojure.lang.IDeref prom
          ^:volatile-mutable ^clojure.lang.PersistentVector ops
          ^:volatile-mutable ^Boolean pending?]
  clojure.lang.IDeref
  (deref [this]
    (if-not pending?
      @prom
      (locking this
        (if-not pending?
          @prom
          (let [ret (if (realized? prom) @prom (reduce realize-step ops))]
            (deliver prom ret)
            (set! ops [])
            (set! pending? false)
            ret)))))
  clojure.lang.IPending
  (isRealized [this] (or (not pending?) (realized? prom)))
  DelayedQueue
  (enqueue-f [this f args]
    (locking this
      (let [p (promise)]
        (if pending?
          (DelayQueue. p (conj ops [p {:fn f :args args}]) true)
          (DelayQueue. p [@prom [p {:fn f :args args}]] true)))))
  (viz [this] {:prom prom :ops ops :pending? pending?}))

(defn delay-queue? [x] (instance? DelayQueue x))
(defn delay-queue
  ([x]
   (cond (delay-queue? x) x
         (instance? clojure.lang.IDeref x) (DelayQueue. (promise) [x {:fn deref :args ()}] true)
         :else (DelayQueue. (deliver (promise) x) [] false)))
  ([x f & args] (apply enqueue-update (delay-queue x) f args)))
