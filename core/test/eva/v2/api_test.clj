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

(ns eva.v2.api-test
  (:require [clojure.test :refer :all]
            [eva.v2.database.index-manager :refer [global-index-cache]]
            [eva.api :refer :all]
            [eva.query.datalog.protocols :as dp]
            [eva.config :as config]
            [eva.v2.messaging.address :as address]
            [eva.v2.storage.block-store.types :as store-type]
            [eva.v2.storage.block-store.impl.memory :as memory]
            [eva.v2.storage.block-store.impl.sql :as sql]
            [eva.v2.storage.value-store.core :as values]
            [eva.v2.storage.value-store.manager :as vs-manager]
            [eva.v2.system.peer-connection.core :as peer]
            [eva.v2.system.transactor.core :as transactor]
            [eva.v2.system.database-catalogue.core :as catalog]
            [eva.v2.system.indexing.core :as indexing]
            [eva.v2.messaging.jms.alpha.local-broker :as broker]
            [eva.v2.database.core :as database]
            [eva.v2.storage.block-store.impl.sql :as sql]
            [eva.v2.server.transactor-test-utils :refer [with-local-mem-connection]]
            [quartermaster.core :as qu]
            [eva.quartermaster-patches :as qp]
            [eva.error :as err]
            [com.stuartsierra.component :as c]
            [recide.core :as rc])
  (:import [eva Database]
           [eva Util]
           [eva.error.v1 EvaException EvaErrorCode]
           [java.util.function Function]
           [java.util UUID]
           (java.io File)))

(defn test-query [_ _ connection]
  (let [init-db (db connection)]
          (is (= #{[0]}
                 (set (q '[:find ?e :where [?e :db/ident :db.part/db]]
                         init-db))
                 (set (q (pr-str '[:find ?e :where [?e :db/ident :db.part/db]])
                         init-db))
                 (set (q '[:find ?e :where [?e 3 :db.part/db]]
                         init-db))
                 (set (q (pr-str '[:find ?e :where [?e 3 :db.part/db]])
                         init-db))))
          (is (= #{[:db/ident]}
                 (set (q '[:find ?a :in $ ?a :where [0 ?a :db.part/db]]
                         init-db
                         :db/ident))))
          (is (= #{[17] [19] [20]}
                 (set (q '[:find ?e :where [?e :db/cardinality :db.cardinality/many]]
                         init-db))))
          (is (= #{[4] [7] [13] [6] [9] [3] [8] [5] [11] [15] [41]}
                 (set (q '[:find ?e :where [?e :db/cardinality :db.cardinality/one]]
                         init-db))))
          (is (= #{:db.install/partition, :db/cardinality, :db/unique, :db/index, :db.install/attribute,
                   :db/doc, :db.install/valueType, :db/txInstant, :db/valueType, :db/fn, :db/isComponent,
                   :db/ident, :db/fulltext, :db/noHistory}
                 (into #{} (q '[:find [?attr-ident ...]
                                :where [:db.part/db :db.install/attribute ?a]
                                       [?a :db/ident ?attr-ident]]
                              init-db))
                 (into #{} (q (pr-str '[:find [?attr-ident ...]
                                        :where [:db.part/db :db.install/attribute ?a]
                                               [?a :db/ident ?attr-ident]])
                              init-db))))))

(defn base-config
  [database-id storage-config messenger-config]
  (merge {::address/transaction-submission "submit-addr"
          ::address/transaction-publication "pub-addr"
          ::address/index-updates "indexes"
          ::peer/id (java.util.UUID/randomUUID)
          ::transactor/id (java.util.UUID/randomUUID)
          ::indexing/id (java.util.UUID/randomUUID)
          ::database/id database-id}
         storage-config
         messenger-config))

(defn messenger-config
  []
  {:messenger-node-config/type :broker-uri
   :broker-uri (broker/broker-uri)})

(defn memory-config
  [database-id]
  {::store-type/storage-type ::store-type/memory
   ::memory/store-id database-id
   ::values/partition-id (java.util.UUID/randomUUID)})

(defn h2-config
  [database-id]
  {::store-type/storage-type ::store-type/sql
   ::values/partition-id     database-id
   ::sql/db-spec             (sql/h2-db-spec (File/createTempFile "test" "h2"))})

(defn test-stuff
  [database-id storage-config messenger-config test-fn]
  (let [config (base-config database-id storage-config messenger-config)
        vs (qu/acquire vs-manager/value-store-manager :test-stuff config)
        database-info (catalog/initialize-database* vs database-id)
        transactor (qu/acquire transactor/transactor-manager :test-stuff config)
        indexer (qu/acquire indexing/indexer-manager :test-stuff config)
        connection (connect config)]
    (try
      (test-fn @transactor @indexer connection)
      (finally
        (qu/release transactor true)
        (qu/release indexer true)
        (release connection)
        (broker/stop-broker!)
        (qu/release vs true)))))

(deftest unit:query
  (testing "with in-memory connection"
    (let [database-id (UUID/randomUUID)]
      (qp/testing-for-resource-leaks
       (test-stuff database-id
                   (memory-config database-id)
                   (messenger-config)
                   test-query))))
  (testing "with transactor and local-storage"
    (let [database-id (UUID/randomUUID)]
      (qp/testing-for-resource-leaks
       (test-stuff database-id
                   (h2-config database-id)
                   (messenger-config)
                   test-query)))))

(deftest unit:log-tx-range
  (let [database-id (UUID/randomUUID)]
    (qp/testing-for-resource-leaks
     (test-stuff database-id
                 (memory-config database-id)
                 (messenger-config)
                 (fn [_ _ connection]
                   (dotimes [i 3]
                     @(transact connection [[:db/add (tempid :db.part/user) :db/doc (str "new thing " i)]]))
                   (let [txr
                         (.txRange ^eva.Log (.log ^eva.Connection connection) 0 10)]
                     (is (= 4 (count txr)))
                     (is (= 0 (:t (first txr))))
                     (is (= 2 (-> txr second :data count)))))))))

(deftest unit:datoms-and-query-truthiness
  (let [database-id (UUID/randomUUID)]
    (qp/testing-for-resource-leaks
     (test-stuff database-id
                 (memory-config database-id)
                 (messenger-config)
                 (fn [_ _ connection]
                   (let [schema [{:db/id #db/id[:db.part/db]
                                  :db.install/_attribute :db.part/db
                                  :db/ident :cone/bool
                                  :db/cardinality :db.cardinality/one
                                  :db/valueType :db.type/boolean}]
                         _ @(transact connection schema)
                         tid1 (tempid :db.part/user)
                         tid2 (tempid :db.part/user)
                         tx [{:db/id tid1
                              :db/ident :foo
                              :cone/bool true}
                             {:db/id tid2
                              :db/ident :bar
                              :cone/bool false}]
                         tres @(transact connection tx)
                         db (:db-after tres)
                         [pid1 pid2] (map (resolve-tempid db (:tempids tres)) [tid1 tid2])]
                     (are [res spec]
                         (= res (set (map :v (apply datoms db :avet spec))))
                       #{true false} [:cone/bool]
                       #{}           [:cone/bool nil]
                       #{true}       [:cone/bool true]
                       #{false}      [:cone/bool false])
                     (are [res spec]
                         (= res (set (dp/extensions db [spec])))
                       #{[pid1 :cone/bool]
                         [pid2 :cone/bool]}       '[?e :cone/bool]
                       #{[pid1 :cone/bool true]}  '[?e :cone/bool true]
                       #{[pid2 :cone/bool false]} '[?e :cone/bool false])
                     (are [res query]
                         (= res (set (q query db)))
                       #{[pid1 true]
                         [pid2 false]} '[:find ?e ?v :where [?e :cone/bool ?v]]
                       #{[pid1]}  '[:find ?e :where [?e :cone/bool true]]
                       #{[pid2]} '[:find ?e :where [?e :cone/bool false]]
                       ;;#{}       nil ;; TODO: do we want to handle this case?
                       )))))))

(deftest unit:lookup-references
  (let [database-id (UUID/randomUUID)]
    (qp/testing-for-resource-leaks
     (test-stuff database-id
                 (memory-config database-id)
                 (messenger-config)
                 (fn [_ _ connection]
                   (let [^Database dbase (db connection)]
                     (is (thrown-with-msg? EvaException
                                           #"cannot process lookup reference with nil value"
                                           (.entid dbase [:db/ident nil])))
                     (is (thrown-with-msg? EvaException
                                           #"cannot process lookup reference with nil attribute"
                                           (.entid dbase [nil 3])))
                     (is (thrown-with-msg? EvaException
                                           #"cannot process lookup reference with nil attribute"
                                           (.entid dbase [nil nil])))
                     (is (thrown-with-msg? Exception
                                           #"lookup references are only allowed for unique attributes"
                                           (.entid dbase [:db/doc "foo"])))
                     (is (thrown-with-msg? EvaException
                                           #"Invalid type for entity coercion"
                                           (entid dbase [":db/ident" :db/ident])))

                     (is (nil? (.entid dbase [:doesnt-exist "foo"])))
                     (is (= 3 (entid dbase [:db/ident :db/ident])))
                     (is (= nil (entid dbase [:db/ident :doesnt-exist])))))))))

(deftest unit:in-mem-db-evicts-indexes
  (let [database-id (UUID/randomUUID)
        connect-cache-count (count @(:cache-atom global-index-cache))]
    (qp/testing-for-resource-leaks
     (test-stuff database-id
                 (memory-config database-id)
                 (messenger-config)
                 (fn [_ _ connection]
                   (is (= (inc connect-cache-count)
                          (count @(:cache-atom global-index-cache)))))))
    (is (= connect-cache-count
           (count @(:cache-atom global-index-cache))))))

(deftest unit:transaction-timeout
  (let [database-id (UUID/randomUUID)]
    (qp/testing-for-resource-leaks
     (config/with-overrides {:eva.transact-timeout 0}
       (test-stuff database-id
                   (memory-config database-id)
                   (messenger-config)
                   (fn [_ _ connection]
                     (err/is-thrown? {:error-codes #{EvaErrorCode/TRANSACTION_TIMEOUT,
                                                     EvaErrorCode/TIMEOUT_ERROR}
                                      :msg-re #"Peer timed out:"
                                      :eva-code 6002
                                      :http-status 504}
                                     @(transact connection []))))))))

(deftest util-reader
  (is (= [3 3]
         (Util/read (java.util.HashMap.
                     ^java.util.Map {"foo/bar" (reify Function (apply [_ x] (inc x)))
                                     "foo.bar/baz" (reify Function (apply [_ x] (dec x)))})
                    "[#foo/bar 2 #foo.bar/baz 4]"))))


(deftest connect-config-can-be-non-clj-map
  (qp/testing-for-resource-leaks
   (let [conn (connect (java.util.HashMap. {:local true}))]
     (is conn)
     (release conn))))

(deftest as-of-basis-t-always-returns-the-tx-num-even-if-constructed-from-eid
  (qp/testing-for-resource-leaks
   (let [conn (connect {:local true})
         tx-res @(transact conn [[:db/add (tempid :db.part/user) :db/doc "foo"]])
         snap (as-of (:db-after tx-res) (to-tx-eid 0))]
     (is (= 0 (as-of-t snap)))
     (release conn))))

(deftest ints-are-upcast-to-longs-for-eids
  (qp/testing-for-resource-leaks
   (let [conn (connect {:local true})]
     (is (instance? Long (.entid (db conn) (int 0))))
     (is (instance? Long (.entidStrict (db conn) (int 0))))
     (is (instance? Long (first (.entids (db conn) [(int 0)]))))
     (is (instance? Long (first (.entidsStrict (db conn) [(int 0)]))))
     (release conn))))

(deftest with-rethrows-tx-exceptions
  (qp/testing-for-resource-leaks
   (let [conn (connect {:local true})]
     (try
       (is (thrown-with-msg? Exception #"Comparison failed for CAS: aborting CAS"
                             (try @(with (db conn) [[:db.fn/cas :db.part/db :db/doc "foo" "bar"]])
                                  (catch Exception e (throw (.getCause e))))))
          (finally (release conn))))))

(deftest with-can-be-used-to-batch-ordered-subtransactions
  (qp/testing-for-resource-leaks
   (let [schema [{:db/id (tempid :db.part/db)
                  :db.install/_attribute :db.part/db
                  :db/ident :stack/contains
                  :db/valueType :db.type/ref
                  :db/cardinality :db.cardinality/many}

                 {:db/id (tempid :db.part/db)
                  :db.install/_attribute :db.part/db
                  :db/ident :stack/id
                  :db/valueType :db.type/string
                  :db/cardinality :db.cardinality/one
                  :db/unique :db.unique/identity}

                 {:db/id (tempid :db.part/db)
                  :db.install/_attribute :db.part/db
                  :db/ident :stack/count
                  :db/valueType :db.type/long
                  :db/cardinality :db.cardinality/one}

                 {:db/id (tempid :db.part/db)
                  :db.install/_attribute :db.part/db
                  :db/ident :stack/position
                  :db/valueType :db.type/long
                  :db/cardinality :db.cardinality/one}

                 {:db/id (tempid :db.part/db)
                  :db/ident :stack/create
                  :db/fn (function '{:lang "clojure"
                                     :params [db stack-id]
                                     :code [{:db/id (d/tempid :db.part/user)
                                             :stack/id stack-id
                                             :stack/count 0}]})}

                 {:db/id (tempid :db.part/db)
                  :db/ident :stack/push
                  :db/fn (function '{:lang "clojure"
                                     :params [db stack-id entity-id]
                                     :code (let [stack-count (-> (d/datoms db :eavt stack-id :stack/count)
                                                                 first
                                                                 :v)]
                                             [{:db/id stack-id
                                               :stack/count (inc stack-count)
                                               :stack/contains entity-id}
                                              {:db/id entity-id
                                               :stack/position stack-count}])})}

                 {:db/id (tempid :db.part/db)
                  :db/ident :stack/pop
                  :db/fn (function '{:lang "clojure"
                                     :params [db stack-id]
                                     :code (let [stack-count (-> (d/datoms db :eavt stack-id :stack/count)
                                                                 first
                                                                 :v)
                                                 stack-position (dec stack-count)
                                                 find-at-position '[:find ?stack-element .
                                                                    :in $ ?stack-id ?stack-position
                                                                    :where
                                                                    [?stack-id :stack/contains ?stack-element]
                                                                    [?stack-element :stack/position ?stack-position]]

                                                 eid-to-pop (d/q find-at-position db stack-id stack-position)]
                                             [[:db/retract stack-id :stack/contains eid-to-pop]
                                              [:db/retract eid-to-pop :stack/position stack-position]
                                              [:db/add stack-id :stack/count stack-position]])})}


                 {:db/id (tempid :db.part/db)
                  :db/ident :stack/batch
                  :db/fn (function '{:lang "clojure"
                                     :requires [[clojure.set :as clj-set]]
                                     :params [init-db root-id stack-tx-ops]
                                     :code

                                     ;; NOTES: This procedure *almost* generalizes, however there are
                                     ;;        some design considerations which may not generalize.
                                     ;;        Off the top of my head:
                                     ;;
                                     ;; 1) How do we scope to the apropriate set of entitity ids for deriving
                                     ;;    our diff?
                                     ;;    -> Simplifying assumption for this test: users know apriori
                                     ;;       a root eid they will care about, and any additional eids can be discovered
                                     ;;       given this root id and the final snapshot.
                                     ;;
                                     ;; 2) How do we resolve the ids of interest so they'll be resolvable against old
                                     ;;    or new database snapshots?
                                     ;;    -> Simplifying assumption for this test: all eids will be directly
                                     ;;       resolvable against the final database state.
                                     ;;
                                     ;; 3) How do we transform the perm-ids (which might not exist in the
                                     ;;    init db) back into tempids?
                                     ;;    -> Simplifying assumption for this test: we can map all non-extant
                                     ;;       permanent ids directly back into tempids in the user partition
                                     ;;
                                     ;; 4) How do we handle the potential of multiple transaction enttiy ids
                                     ;;    of interest (which will *not* be resolvable to distinct eids)?
                                     ;;    -> Simplifying assumption for this test: we don't care about tx-eids.

                                     (letfn [(relevant-eids [final-db root-id]
                                               (let [children (->> (d/datoms final-db :eavt root-id :stack/contains)
                                                                   (map :v))]
                                                 (conj children (d/entid final-db root-id))))
                                             (diff-over-eids
                                               [db1 db2 eids]

                                               (let [diff '[:find ?e ?a ?v
                                                            :in $db1 $db2 [?e ...]
                                                            :where
                                                            [$db1 ?e ?a ?v]
                                                            (not [$db2 ?e ?a ?v])]]
                                                 [(d/q diff db1 db2 eids)
                                                  (d/q diff db2 db1 eids)]))

                                             (tempify-datom [transformation-map final-db [op e a v]]
                                               [op
                                                (transformation-map e)
                                                a
                                                (if (= :db.type/ref (eva.attribute/value-type (d/attribute final-db a)))
                                                  (transformation-map v v)
                                                  v)])]

                                       (let [speculative-transaction-results
                                             (->> (reductions (fn [tx-res tx-data] (d/with (:db-after tx-res) tx-data))
                                                              {:db-after init-db}
                                                              stack-tx-ops)
                                                  rest
                                                  (doall))

                                             final-db (->> speculative-transaction-results last :db-after)

                                             resolved-eids (relevant-eids final-db root-id)

                                             [only-in-init only-in-final] (diff-over-eids init-db final-db resolved-eids)

                                             tempification-map (zipmap resolved-eids
                                                                       (repeatedly #(d/tempid :db.part/user)))
                                             compiled-tx (concat (for [[e a v] only-in-init]
                                                                   (tempify-datom tempification-map final-db [:db/retract e a v]))
                                                                 (for [[e a v] only-in-final]
                                                                   (tempify-datom tempification-map final-db [:db/add e a v])))]
                                         compiled-tx))})}]
         conn (connect {:local true})]
     (try
       ;; NOTE: forcing the indexes to flush forces a previously incorrect
       ;;       state to occur on the transactor wherein `with` snapshots
       ;;       leak to the in-mem database snapshot cache. Please
       ;;       keep these transactions here.
       (dotimes [n 200] @(transact conn []))
       @(transact conn schema)
       @(transact conn [[:stack/create "test-stack-one"]])
       @(transact conn [[:stack/push [:stack/id "test-stack-one"] (tempid :db.part/user -1)]
                        {:db/id (tempid :db.part/user -1) :db/doc "A"}])
       @(transact conn [[:stack/push [:stack/id "test-stack-one"] (tempid :db.part/user -1)]
                        {:db/id (tempid :db.part/user -1) :db/doc "B"}])
       @(transact conn [[:stack/push [:stack/id "test-stack-one"] (tempid :db.part/user -1)]
                        {:db/id (tempid :db.part/user -1) :db/doc "C"}])
       @(transact conn [[:stack/pop [:stack/id "test-stack-one"]]])
       @(transact conn [[:stack/push [:stack/id "test-stack-one"] (tempid :db.part/user -1)]
                        {:db/id (tempid :db.part/user -1) :db/doc "D"}])


       (let [batch-tx-res @(transact conn [[:stack/batch
                                            [:stack/id "test-stack-two"]
                                            [[[:stack/create "test-stack-two"]]
                                             [[:stack/push [:stack/id "test-stack-two"] (tempid :db.part/user -1)]
                                              {:db/id (tempid :db.part/user -1) :db/doc "A"}]
                                             [[:stack/push [:stack/id "test-stack-two"] (tempid :db.part/user -1)]
                                              {:db/id (tempid :db.part/user -1) :db/doc "B"}]
                                             [[:stack/push [:stack/id "test-stack-two"] (tempid :db.part/user -1)]
                                              {:db/id (tempid :db.part/user -1) :db/doc "C"}]
                                             [[:stack/pop [:stack/id "test-stack-two"]]]
                                             [[:stack/push [:stack/id "test-stack-two"] (tempid :db.part/user -1)]
                                              {:db/id (tempid :db.part/user -1) :db/doc "D"}]]]])
             stack-spec [:stack/count {:stack/contains [:stack/position :db/doc]}]
             final-db (db conn)]

         (is (= (->> (pull final-db stack-spec [:stack/id "test-stack-one"])
                     :stack/contents
                     (sort-by :stack/position))

                (->> (pull final-db stack-spec [:stack/id "test-stack-two"])
                     :stack/contents
                     (sort-by :stack/position)))))

       (finally (release conn))))))

(deftest jira-eva-170-read-api-consistency-tests
  (with-local-mem-connection conn
    (let [db (db conn)
          coercion-exception #{EvaErrorCode/COERCION_FAILURE EvaErrorCode/API_ERROR}]

      (testing "consistency of read endpoint "
        (testing "datoms"
          (testing "nominal reads work as expected"
            (is (= [0 3 :db.part/db] ((juxt :e :a :v) (first (datoms db :eavt 0 3 :db.part/db))))))

          (testing "empty reads work as expected"
            (are [x y] (= x y)
              () (datoms db :eavt 82)
              () (datoms db :aevt :db/noHistory)))

          (testing "reasonable, but unresolvable, reads return equivalent to empty reads"
            (are [x y] (= x y)
              ;; unresolvable attributes
              () (datoms db :eavt 0 :doesnt-exist)
              () (datoms db :aevt :also-does-not-exist)
              () (datoms db :avet :this-either)
              () (datoms db :vaet 0 :nope)

              ;; special case for datoms: nil passed in directly
              () (datoms db :eavt nil)
              () (datoms db :eavt 0 3 :db.part/db nil)))

          ;; TODO: specify eva exception subtypes for contract / tests
          (testing "obvious garbage types in throw eva exceptions"
            (err/is-thrown? {} (datoms nil :vaet))
            (err/is-thrown? {} (datoms db nil))
            (err/is-thrown? {} (datoms db :foo))
            (err/is-thrown? {} (datoms db :eavt 'some-symbol))
            (err/is-thrown? {} (datoms db :eavt (java.util.HashMap.)))))

        (testing "query"
          (testing "nominal reads work as expected"
            (is (= 0 (q '[:find ?x . :where [?x :db/ident :db.part/db]] db))))

          (testing "empty reads work as expected"
            (are [x y] (= x (q y db))
              nil '[:find ?x . :where [?x :db/ident :foo]]
              []  '[:find [?x] :where [?x :db/ident :foo]]
              []  '[:find [?x ...] :where [?x :db/ident :foo]]
              []  '[:find ?x :where [?x :db/ident :foo]]))

          (testing "reasonable, but unresolvable, reads return equivalent to empty reads"
            (are [x y] (= x (q y db))
              nil '[:find ?x . :where [?x :foo]]
              []  '[:find [?x] :where [?x :bar]]
              []  '[:find [?x ...]  :where [?x :db/ident :db.part/db] [?x :does-not-exist]]
              []  '[:find ?x :where [?x :db/valueType :foo]]))

          (testing "obvious garbage types in throw eva exceptions"
            (are [y] (thrown? EvaException (q y db))
              "Foo"
              #{1 2 3})))

        (testing "pull"
          (testing "nominal reads work as expected"
            (are [x y z] (= x (pull db y z))
              {:db/id 3} [:db/id] :db/ident))

          (testing "empty reads work as expected"
            (are [x y z] (= x (pull db y z))
              {:db/id 3} [:db/id :db/noHistory] :db/ident))

          (testing "reasonable, but unresolvable, reads return equivalent to empty reads"
            (are [x y z] (= x (pull db y z))
              {:db/id 3} [:db/id :foo] :db/ident))

          (testing "obvious garbage types in throw eva exceptions"
            (are [y] (thrown? EvaException y)
              (pull db "foo" :db/ident)
              (pull db [:db/id] "foo"))))

        (testing "entity"
          (let [ident-ent (entity db :db/ident)]
            (testing "nominal reads work as expected"
              (are [x y] (= x y)
                :db/ident (:db/ident ident-ent)))

            (testing "empty reads work as expected"
              (are [x y] (= x y)
                nil (:db/noHistory ident-ent)))

            (testing "reasonable, but unresolvable, reads return equivalent to empty reads"
              (are [x y] (= x y)
                nil (:foo ident-ent)))

            (testing "obvious garbage types in throw eva exceptions"
              ;; TODO: I'm not actually sure what qualifies as garbage here, since
              ;;       the entities work as associative collections where 'get' will
              ;;       just return nil for anything weird type-wise
              )))

        (testing "extant-entity?"
          (testing "nominal reads work as expected"
            (is (true? (.isExtantEntity db :db/ident)))
            (is (true? (.isExtantEntity db [:db/ident :db.part/db])))
            (is (true? (.isExtantEntity db 17))))

          (testing "empty reads work as expected"
            (is (false? (.isExtantEntity db 123))))

          (testing "reasonable, but unresolvable, reads return equivalent to empty reads..."
            (is (false? (.isExtantEntity db :foo)))
            (is (false? (.isExtantEntity db [:foo "bar"]))))

          (testing "obvious garbage types in throw eva exceptions"
            (is (thrown? EvaException (.isExtantEntity db "strings are bad mmkay"))))))

      (testing "consistent behavior for coercion endpoint"
        (testing "consistent behavior for entid and entids"
          (testing "nominal reads work as expected"
            (are [x y] (= x y)
              1234 (.entid db 1234)
              1234 (.entid db (int 1234))
              0 (.entid db :db.part/db)
              1 (.entid db [:db/ident :db.part/tx]))

            (is (= [1234 1234 0 1]
                   (.entids db [1234 (int 1234) :db.part/db [:db/ident :db.part/tx]]))))

          (testing "empty reads work as expected"
            ;; NOTE: as a 'scalar' resolution endpoint, entid doesn't have 'empty'
            ;;       reads as a distinct class, only resolvable or unresolvable ones.
            (is (= [] (.entids db [])))
            (is (= [] (.entidsStrict db []))))

          (testing "reasonable, but unresolvable, reads return equivalent to empty reads..."
            (is (nil? (.entid db :undefined/ident)))
            (is (nil? (.entid db [:foo "bar"])))

            (is (= [nil nil]
                   (.entids db [:undefined/ident [:foo "bar"]]))))

          (testing "...but the strict variants should throw in these same instances."
            (err/is-thrown? {:error-codes coercion-exception} (.entidStrict db :undefined/ident))
            (err/is-thrown? {:error-codes coercion-exception} (.entidStrict db [:foo "bar"]))
            (err/is-thrown? {:error-codes coercion-exception} (.entidsStrict db [:undefined/ident])))

          (testing "obvious garbage types in throw eva exceptions"
            (err/is-thrown? {:error-codes coercion-exception} (.entid db 'some-symbol))
            (err/is-thrown? {:error-codes coercion-exception} (.entids db ['some-symbol]))))

        (testing "ident"
          (testing "nominal reads work as expected"
            (is (= :db/ident (.ident db 3))))

          (testing "empty reads work as expected"
            ;; NOTE: as a 'scalar' resolution endpoint, ident doesn't have 'empty'
            ;;       reads as a distinct class, only resolvable or unresolvable ones.
            )

          (testing "reasonable, but unresolvable, reads return equivalent to empty reads..."
            (is (nil? (.ident db 123))))

          (testing "...but the strict variants should throw in these same instances"
            (err/is-thrown? {:error-codes coercion-exception} (.identStrict db 123)))

          (testing "obvious garbage types in throw eva exceptions"
            (err/is-thrown? {:error-codes coercion-exception} (.ident db "foo"))))

        (testing "attribute"
          (testing "nominal reads work as expected"
            (is (instance? eva.Attribute (.attribute db :db.install/attribute))))

          (testing "empty reads work as expected"
            ;; NOTE: as a 'scalar' resolution endpoint, attribute doesn't have 'empty'
            ;;       reads as a distinct class, only resolvable or unresolvable ones.
            )

          (testing "reasonable, but unresolvable, reads return equivalent to empty reads..."
            (is (nil? (.attribute db :not-an-installed-attribute))))

          (testing "...but the strict variants should throw in these same instances"
            (err/is-thrown? {:error-codes coercion-exception} (.attributeStrict db :not-an-installed-attribute)))

          (testing "obvious garbage types in throw eva exceptions"
            (err/is-thrown? {:error-codes coercion-exception} (.attribute db "totally-not-a-keyword"))))))))
