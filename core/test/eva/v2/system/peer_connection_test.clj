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

(ns eva.v2.system.peer-connection-test
  (:require [clojure.test :refer :all]
            [eva.api :as eva :refer [connect release]]
            [quartermaster.core :as qu]
            [eva.quartermaster-patches :as qp]
            [eva.v2.storage.block-store.types :as store-type]
            [eva.v2.storage.value-store.core :as values]
            [eva.v2.storage.value-store.manager :as vs-manager]
            [eva.v2.storage.block-store.impl.memory :as memory]
            [eva.v2.system.database-catalogue.core :as db-ctlg]
            [eva.v2.system.peer-connection.core :as peer]
            [eva.v2.system.transactor.core :as transactor]
            [eva.v2.system.indexing.core :as indexing]
            [eva.v2.database.core :as database]
            [eva.v2.messaging.jms.alpha.local-broker :as broker]
            [eva.v2.messaging.address :as address]))

(deftest test:initial-test
  (let [broker-uri (broker/broker-uri)
        messenger-config {:messenger-node-config/type :broker-uri
                          :broker-uri broker-uri}
        database-id (random-uuid)
        storage-config {::store-type/storage-type ::store-type/memory
                        ::memory/store-id database-id
                        ::values/partition-id (random-uuid)}
        base-config (merge {::address/transaction-submission "submit-addr"
                            ::address/transaction-publication "pub-addr"
                            ::address/index-updates "indexes"
                            ::peer/id (random-uuid)
                            ::transactor/id (random-uuid)
                            ::indexing/id (random-uuid)
                            ::database/id database-id}
                           storage-config
                           messenger-config)
        vs (qu/acquire vs-manager/value-store-manager :testing base-config)
        database-info (db-ctlg/initialize-database* vs database-id)
        txor (qu/acquire transactor/transactor-manager :testing base-config)
        idxr (qu/acquire indexing/indexer-manager :testing base-config)
        conn (connect base-config)]
    (try
      [(.asOf (eva/db conn) 0)
       (doall (for [n (range 10)] @(.transact conn [])))
       (:state (:database-connection conn))
       (eva.api/db conn)]
      (finally
        (qu/release txor true)
        (qu/release idxr true)
        (release conn)
        (qu/release vs true)
        (broker/stop-broker!)))))

(defn cur-range [window-size tx-num]
  (range (max 0 (inc (- tx-num window-size)))
         (inc tx-num)))

(defn gen-expected [tx-size window-size tx-num]
  (vec (for [i (cur-range window-size tx-num)]
         (assoc {:tx-num i}
                :children (set (take tx-size (map #(do {:child-num %}) (range))))))))

(defn expected-tx-num-frequencies [tx window-size]
  (cond-> {true tx}
    (> tx window-size) (assoc false (- tx window-size))))

(defn expected-children-frequencies [tx window-size tx-size]
  (cond-> (update (expected-tx-num-frequencies tx window-size) true #(* % tx-size))
    (> tx window-size) (update false #(* % tx-size))))

(defn gen-tx-add [tx-size tx-num]
  [{:db/id (eva/tempid :db.part/user)
    :tx-num tx-num
    :children (take tx-size (map #(do {:db/id (eva/tempid :db.part/user) :child-num %}) (range)))}])

(defn gen-tx-retract [window-size tx-num]
  (let [thresh (- tx-num window-size)]
    (when (>= thresh 0)
      [[:db.fn/retractEntity [:tx-num thresh]]])))

(defn gen-tx [tx-size window-size tx-num]
  (concat (gen-tx-add tx-size tx-num)
          (gen-tx-retract window-size tx-num)))

(deftest sliding-window-consistency-v2
  (qp/testing-for-resource-leaks
   (eva.config/with-overrides {:eva.database.indexes.max-tx-delta 30
                               :eva.v2.storage.value-cache-size 1}
     (let [broker-uri (broker/broker-uri)
           messenger-config {:messenger-node-config/type :broker-uri
                             :broker-uri broker-uri}
           database-id (random-uuid)
           storage-config {::store-type/storage-type ::store-type/memory
                           ::memory/store-id database-id
                           ::values/partition-id (random-uuid)}
           full-config (merge {::address/transaction-submission "submit-addr"
                               ::address/transaction-publication "pub-addr"
                               ::address/index-updates "indexes"
                               ::peer/id (random-uuid)
                               ::transactor/id (random-uuid)
                               ::indexing/id (random-uuid)
                               ::database/id database-id}
                              storage-config
                              messenger-config)
           value-store (qu/acquire vs-manager/value-store-manager :testing full-config)
           database-info (db-ctlg/initialize-database* value-store database-id)
           txor (qu/acquire transactor/transactor-manager :testing full-config)
           idxr (qu/acquire indexing/indexer-manager :testing full-config)
           conn (connect full-config)]
       (try (let [txs         100
                  tx-size     4
                  window-size 6
                  check-every 1
                  schema [{:db.install/_attribute :db.part/db
                           :db/id (eva/tempid :db.part/db)
                           :db/ident :children
                           :db/valueType :db.type/ref
                           :db/isComponent true
                           :db/cardinality :db.cardinality/many}
                          {:db.install/_attribute :db.part/db
                           :db/id (eva/tempid :db.part/db)
                           :db/ident :tx-num
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity}
                          {:db.install/_attribute :db.part/db
                           :db/id (eva/tempid :db.part/db)
                           :db/ident :child-num
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}]]
              @(eva/transact conn schema)
              (dorun
               (map-indexed
                (fn step [i tx]
                  #_(println "sliding window:" i)
                  (let [res (deref (eva/transact conn tx) 10000 :timedout)]
                    (when (= 0 (mod i check-every))
                      (let [after ^eva.Database (:db-after res)
                            r (rand-int i)
                            start (. java.lang.System (clojure.core/nanoTime))
                            rand-db ^eva.Database (eva/as-of ^Database after (inc (inc r)))
                            duration (- (. java.lang.System (clojure.core/nanoTime)) start)]
                        (is (= (.latestT conn) (inc (inc i))))
                        ;; Check that the current instance is correct
                        (is (= (gen-expected tx-size window-size i)
                               (map
                                (fn [m] (update m :children set))
                                (eva/pull-many after
                                               '[:tx-num {:children [:child-num]}]
                                               (map #(do [:tx-num %]) (cur-range window-size i))))))
                        (is (= (set (cur-range window-size i))
                               (set (eva/q '[:find [?tx-num ...]
                                             :where [_ :tx-num ?tx-num]]
                                           after))))
                        (is (= (.snapshotT after) (.basisT after) (inc (inc i))))
                        (is (nil? (.asOfT after)))
                        ;; check the current historical indexes
                        (is (= (expected-tx-num-frequencies (inc i) window-size)
                               (frequencies (map :added (eva/datoms (eva/history after) :aevt :tx-num)))))
                        (is (= (expected-children-frequencies (inc i) window-size tx-size)
                               (frequencies (map :added (eva/datoms (eva/history after) :aevt :children)))
                               (frequencies (map :added (eva/datoms (eva/history after) :aevt :child-num)))))
                        ;; Check that a random point-in-time is correct
                        (is (= (gen-expected tx-size window-size r)
                               (map
                                (fn [m] (update m :children set))
                                (eva/pull-many rand-db
                                               [:tx-num {:children [:child-num]}]
                                               (map #(do [:tx-num %]) (cur-range window-size r))))))
                        (is (= (set (cur-range window-size r))
                               (set (eva/q '[:find [?tx-num ...]
                                             :where [_ :tx-num ?tx-num]]
                                           rand-db))))
                        (is (= (.basisT rand-db) (inc (inc i))))
                        (is (= (.snapshotT rand-db) (inc (inc r))))
                        (is (or (= (.asOfT rand-db) (inc (inc r)))
                                (= i r)))
                        ;; check the as-of historical indexes
                        (is (= (expected-tx-num-frequencies (inc r) window-size)
                               (frequencies (map :added (eva/datoms (eva/history rand-db) :aevt :tx-num)))))
                        (is (= (expected-children-frequencies (inc r) window-size tx-size)
                               (frequencies (map :added (eva/datoms (eva/history rand-db) :aevt :children)))
                               (frequencies (map :added (eva/datoms (eva/history rand-db) :aevt :child-num)))))))))
                (take txs (map (partial gen-tx tx-size window-size) (range))))))
            (finally
              (qu/release txor true)
              (qu/release idxr true)
              (release conn)
              (qu/release value-store true)
              (broker/stop-broker!)))))))


(deftest sliding-window-consistency-v2-in-mem
  (qp/testing-for-resource-leaks
   (eva.config/with-overrides {:eva.database.indexes.max-tx-delta 30
                               :eva.v2.storage.value-cache-size 1}
     (let [database-id (random-uuid)
           conn (connect {:local true})]
       (try (let [txs         100
                  tx-size     4
                  window-size 6
                  check-every 1
                  schema [{:db.install/_attribute :db.part/db
                           :db/id (eva/tempid :db.part/db)
                           :db/ident :children
                           :db/valueType :db.type/ref
                           :db/isComponent true
                           :db/cardinality :db.cardinality/many}
                          {:db.install/_attribute :db.part/db
                           :db/id (eva/tempid :db.part/db)
                           :db/ident :tx-num
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one
                           :db/unique :db.unique/identity}
                          {:db.install/_attribute :db.part/db
                           :db/id (eva/tempid :db.part/db)
                           :db/ident :child-num
                           :db/valueType :db.type/long
                           :db/cardinality :db.cardinality/one}]]
              @(eva/transact conn schema)
              (dorun
               (map-indexed
                (fn step [i tx]
                  (let [res (deref (eva/transact conn tx) 10000 :timedout)]
                    (when (= 0 (mod i check-every))
                      (let [after ^eva.Database (:db-after res)
                            r (rand-int i)
                            start (. java.lang.System (clojure.core/nanoTime))
                            rand-db ^eva.Database (eva/as-of ^Database after (inc (inc r)))
                            duration (- (. java.lang.System (clojure.core/nanoTime)) start)]
                        (is (= (.latestT conn) (inc (inc i))))
                        ;; Check that the current instance is correct
                        (is (= (gen-expected tx-size window-size i)
                               (map
                                (fn [m] (update m :children set))
                                (eva/pull-many after
                                               '[:tx-num {:children [:child-num]}]
                                               (map #(do [:tx-num %]) (cur-range window-size i))))))
                        (is (= (set (cur-range window-size i))
                               (set (eva/q '[:find [?tx-num ...]
                                             :where [_ :tx-num ?tx-num]]
                                           after))))
                        (is (= (.snapshotT after) (.basisT after) (inc (inc i))))
                        (is (nil? (.asOfT after)))
                        ;; check the current historical indexes
                        (is (= (expected-tx-num-frequencies (inc i) window-size)
                               (frequencies (map :added (eva/datoms (eva/history after) :aevt :tx-num)))))
                        (is (= (expected-children-frequencies (inc i) window-size tx-size)
                               (frequencies (map :added (eva/datoms (eva/history after) :aevt :children)))
                               (frequencies (map :added (eva/datoms (eva/history after) :aevt :child-num)))))
                        ;; Check that a random point-in-time is correct
                        (is (= (gen-expected tx-size window-size r)
                               (map
                                (fn [m] (update m :children set))
                                (eva/pull-many rand-db
                                               [:tx-num {:children [:child-num]}]
                                               (map #(do [:tx-num %]) (cur-range window-size r))))))
                        (is (= (set (cur-range window-size r))
                               (set (eva/q '[:find [?tx-num ...]
                                             :where [_ :tx-num ?tx-num]]
                                           rand-db))))
                        (is (= (.basisT rand-db) (inc (inc i))))
                        (is (= (.snapshotT rand-db) (inc (inc r))))
                        (is (or (= (.asOfT rand-db) (inc (inc r)))
                                (= i r)))
                        ;; check the as-of historical indexes
                        (is (= (expected-tx-num-frequencies (inc r) window-size)
                               (frequencies (map :added (eva/datoms (eva/history rand-db) :aevt :tx-num)))))
                        (is (= (expected-children-frequencies (inc r) window-size tx-size)
                               (frequencies (map :added (eva/datoms (eva/history rand-db) :aevt :children)))
                               (frequencies (map :added (eva/datoms (eva/history rand-db) :aevt :child-num)))))))))
                (take txs (map (partial gen-tx tx-size window-size) (range))))))
            (finally
              (release conn)))))))

(deftest new-autogenic-connections-can-be-created-before-all-are-released
  ;; Regression test for a bug in which new autogenic (local)
  ;; connections cannot be created until all existing connections are released.
  (testing "original bug reproduction"
    (qp/testing-for-resource-leaks
     (let [conn1 (eva/connect {:local true
                               :eva.v2.database.core/id (random-uuid)
                               :eva.v2.storage.value-store.core/partition-id (random-uuid)})

           conn2 (eva/connect {:local true
                               :eva.v2.database.core/id (random-uuid)
                               :eva.v2.storage.value-store.core/partition-id (random-uuid)})]
       (eva/release conn1)
       (let [conn3 (eva/connect {:local true
                                 :eva.v2.database.core/id (random-uuid)
                                 :eva.v2.storage.value-store.core/partition-id (random-uuid)})]
         (eva/release conn2)
         (eva/release conn3)))))

  (testing "variation on original bug"
    (qp/testing-for-resource-leaks
     (let [config {:local true
                   :eva.v2.database.core/id (random-uuid)
                   :eva.v2.storage.value-store.core/partition-id (random-uuid)}
           conn1 (eva/connect config)
           conn2 (eva/connect {:local true
                               :eva.v2.database.core/id (random-uuid)
                               :eva.v2.storage.value-store.core/partition-id (random-uuid)})]
       (eva/release conn1)
       (try
         (let [conn1' (eva/connect config)]
           (eva/release conn1'))
         (finally
           (eva/release conn2)))))))
