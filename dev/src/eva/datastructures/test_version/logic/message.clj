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

(ns eva.datastructures.test-version.logic.message
  (:require [eva.datastructures.test-version.logic.protocols :refer :all :as protocols]
            [eva.datastructures.protocols :refer [interval-contains?
                                                  interval-overlaps?
                                                  low-open?
                                                  high-open?
                                                  low
                                                  high]]
            [eva.datastructures.utils.interval :as interval]
            [clojure.data.avl :as avl]
            [utiliva.comparator :as comparison])
  (:import [eva.datastructures.utils.interval Interval]))

(defrecord BTreeMessage
    [tx-added op target content]
  protocols/TreeMessage
  (op [_] op)
  (recip [_] target)
  (payload [_] content)
  (ranged? [_] (instance? Interval target))
  (apply-message [this cmp kvstore]
    (case op ;; should probably test some time. http://insideclojure.org/2015/04/27/poly-perf/
      :upsert
      (assoc kvstore target content)
      :delete
      (dissoc kvstore target)
      :filter
      (into (empty kvstore)
            (filter (comp #(or (not (interval-contains? target cmp %))
                               (content %))
                          val))
            kvstore)
      :remove-interval
      (let [to-remove (avl/subrange ;; TODO: consider avl agnostic alternative.
                       kvstore
                       (if (low-open? target) > >=)
                       (low target)
                       (if (high-open? target) < <=)
                       (high target))]
        (reduce dissoc kvstore (keys to-remove))))))

(defrecord UpsertMessage
    [tx-added target content]
  protocols/TreeMessage
  (op [_] :upsert)
  (recip [_] target)
  (payload [_] content)
  (ranged? [_] false)
  (apply-message [this cmp kvstore]
    (assoc kvstore target content)))

(defrecord DeleteMessage
    [tx-added target content]
  protocols/TreeMessage
  (op [_] :delete)
  (recip [_] target)
  (payload [_] content)
  (ranged? [_] false)
  (apply-message [this cmp kvstore]
    (dissoc kvstore target)))

(defrecord FilterMessage
    [tx-added target content]
  protocols/TreeMessage
  (op [_] :filter)
  (recip [_] target)
  (payload [_] content)
  (ranged? [_] true)
  (apply-message [this cmp kvstore]
    (into (empty kvstore)
          (filter (comp #(or (not (interval-contains? target cmp %))
                             (content %))
                        val))
          kvstore)))

(defrecord RemoveIntervalMessage
    [tx-added target content]
  protocols/TreeMessage
  (op [_] :remove-interval)
  (recip [_] target)
  (payload [_] content)
  (ranged? [_] true)
  (apply-message [this cmp kvstore]
    (let [to-remove (avl/subrange ;; TODO: consider avl agnostic alternative.
                     kvstore
                     (if (low-open? target) > >=)
                     (low target)
                     (if (high-open? target) < <=)
                     (high target))]
      (reduce dissoc kvstore (keys to-remove)))))

(defn btree-message
  ([op target content]
   (BTreeMessage. nil op target content))
  ([tx-added op target content]
   (BTreeMessage. tx-added op target content)))

(defn upsert-message
  ([k v] (upsert-message nil k v))
  ([tx-added k v] #_(btree-message tx-added :upsert k v)
   (UpsertMessage. tx-added k v)))

(defn delete-message
  ([k] (delete-message nil k))
  ([tx-added k] #_(btree-message tx-added :delete k nil)
   (DeleteMessage. tx-added k nil)))

(defn filter-message
  ([f] (filter-message nil f))
  ([tx-added f] #_(btree-message tx-added :filter interval/infinite-interval f)
   (FilterMessage. tx-added interval/infinite-interval f)))

(defn remove-interval-message
  ([rng] (remove-interval-message nil rng))
  ([tx-added rng] #_(btree-message tx-added :remove-interval rng nil)
   (RemoveIntervalMessage. tx-added rng nil)))

(defn between
  ([start end]
   (fn [msg]
     (let [dest (recip msg)]
       (>= end dest start))))
  ([cmp start end]
   (fn [msg]
     (let [dest (recip msg)]
       (comparison/>= cmp end dest start)))))
