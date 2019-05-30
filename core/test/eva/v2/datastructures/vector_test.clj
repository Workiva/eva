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

(ns eva.v2.datastructures.vector-test
  (:require [clojure.test :refer :all]
            [eva.v2.datastructures.vector :refer :all]
            [quartermaster.core :as qu]
            [eva.datastructures.protocols :as dsp]
            [recide.core :refer [try*]]
            [eva.v2.storage.value-store.core :as value]
            [eva.v2.storage.value-store.concurrent :as vsc]
            [eva.v2.storage.value-store :as vs]
            [eva.v2.storage.block-store.types :as store-type]
            [eva.v2.storage.block-store.impl.memory :as memory])
  (:import [eva.v2.datastructures.vector PersistedVector]
           [java.util UUID]))

(defn maybe-write [name v next-val fail-atom]
  (try*
    (let [vec' (conj v {:name name :val next-val})]
      vec')
    (catch :datastructures/stale e
      (swap! fail-atom inc)
      :write-failed)))

(defn memory-config
  []
  {::store-type/storage-type ::store-type/memory
   ::memory/store-id (UUID/randomUUID)
   ::value/partition-id (UUID/randomUUID)})

(defn open-and-transact [todo {:as agt :keys [vec-id name store fail-atom]}]
  (try*
   (loop [v (open-persisted-vector store vec-id)
          todo todo]
     (if (= todo 0)
       agt
       (let [next-val (inc (:val @(nth v (dec (count v)))))
             maybe-vec (maybe-write name v next-val fail-atom)]
         (if (identical? :write-failed maybe-vec)
           (recur (open-persisted-vector store vec-id) todo)
           (recur maybe-vec (dec todo))))))))

(deftest persisted-vector-concurrency-safety
  (let [storage-config (memory-config)
        store (qu/acquire vsc/concurrent-value-store-manager :testing storage-config)
        init-vector (create-persisted-vector store)
        vec-id (dsp/storage-id init-vector)
        _ (conj init-vector {:name -1 :val 0}) ;; prime the pump for the agents
        num-agents 10
        num-writes-per-agent 100
        total (inc (* num-agents num-writes-per-agent))
        agents (doall (for [i (range num-agents)] (agent {:vec-id vec-id :name i :store store :fail-atom (atom 0)})))]
    (try
      (apply await (doall (for [a agents] (send a (partial open-and-transact num-writes-per-agent)))))
      (let [final-vector (open-persisted-vector store vec-id)
            final-vector-contents (map deref (seq final-vector))
            final-vector-names (map :name final-vector-contents)
            final-vector-values (map :val final-vector-contents)]
        (is (= (inc (* num-agents num-writes-per-agent))
               (count final-vector)))
        (is (= (range 0 total)
               final-vector-values))
        (is (= (frequencies final-vector-names)
               (-> (zipmap (range num-agents)
                           (repeat num-writes-per-agent))
                   (assoc -1 1))))
        (is (> (reduce + (map (comp deref :fail-atom deref) agents)) 0)))
      (finally
        (qu/release* vsc/concurrent-value-store-manager :testing storage-config)))))

(deftest persisted-vector-unit-tests
  (let [storage-config (memory-config)
        store (qu/acquire vsc/concurrent-value-store-manager :testing storage-config)
        named-vec (create-persisted-vector store)
        pv0 (open-persisted-vector store (dsp/storage-id named-vec))]
    (try
      (is (thrown? java.lang.IndexOutOfBoundsException (nth pv0 0)))
      (is (= 0 (count pv0)))
      (is (= 0 (count (seq pv0))))
      (is (= 0 (count (vec pv0))))
      (let [pv1 (conj pv0 1)
            pv2 (conj pv1 2)
            pv3 (conj pv2 3)
            pv (into pv3 [4 5 6])]
        (are [x y] (= x y)
          0 (count pv0)
          1 (count pv1)
          2 (count pv2)
          3 (count pv3)
          6 (count pv)
          1 @(nth pv 0)
          2 @(nth pv 1)
          3 @(nth pv 2)
          4 @(nth pv 3)
          5 @(nth pv 4)
          6 @(nth pv 5)
          [3 4 5] @(read-range pv 2 5)
          []       @(read-range pv 0 0)
          []       (lazy-read-range pv 0 0)
          [1 2 3 4 5 6] @(read-range pv 0 6)
          [1 2 3 4 5 6] (lazy-read-range pv 0 6))
        (is (thrown? java.lang.ClassCastException (compare-and-set! (nth pv 3) 42 4)))
        (is (thrown? java.lang.IndexOutOfBoundsException (read-range pv -1 0)))
        (is (thrown? java.lang.IndexOutOfBoundsException (lazy-read-range pv -1 0))))
      (finally (qu/release* vsc/concurrent-value-store-manager :testing storage-config)))))

(deftest persisted-vector-repair-tests
  (let [storage-config (memory-config)
        store (qu/acquire vsc/concurrent-value-store-manager :testing storage-config)
        init-vec (create-persisted-vector store)
        head-key (dsp/storage-id init-vec)
        built-vec (into init-vec (range 10))]
    (try
      ;; case: vec isn't actually damaged
      (is (= 10 (count (repair-vector-head built-vec store))))
      ;; case: vec is damaged and repaired successfully
      (is (true?
           @(vs/replace-value @(.store ^PersistedVector built-vec) head-key {:count 10} {:count 9})))
      (is (= 10 (count (repair-vector-head built-vec store))))
      ;; case: repair race
      (is (true?
           @(vs/replace-value @(.store ^PersistedVector built-vec) head-key {:count 10} {:count 4})))
      (let [repair-fn #(try* (repair-vector-head built-vec store)
                             :repaired
                             (catch :datastructures/concurrent-modification e
                               :concurrent-exception))
            r1 (future (repair-fn))
            r2 (future (repair-fn))
            r3 (future (repair-fn))
            r4 (future (repair-fn))
            r5 (future (repair-fn))
            r6 (future (repair-fn))]
        ;; NOTE: it is possible for this test to fail if r1 completely finishes
        ;;       before r2+ have a chance to open the vector. Running
        ;;       the test locally a few hundred times, this seems unlikely,
        ;;       but it could happen more consistently on different hardware or
        ;;       different backing stores.
        ;;
        ;;   --> a fix would be to mock a delay into the replace op
        (is (= #{:repaired :concurrent-exception}
               (into #{} (map deref) [r1 r2 r3 r4 r5 r6]))))
      (finally (qu/release* vsc/concurrent-value-store-manager :testing storage-config)))))
