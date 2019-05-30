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

(ns eva.transaction-test
  (:require [eva.api :as eva]
            [eva.entity-id :refer [entid-strict]]
            [eva.core :refer [multi-select-datoms-ordered]]
            [eva.readers]
            [eva.api :refer [connect squuid ident transact]]
            [eva.bytes :as bytes]
            [clojure.tools.logging :refer [debug]]
            [schema.test]
            [clojure.test :refer :all]
            [eva.error :refer [is-thrown?]]
            [eva.v2.server.transactor-test-utils
             :refer [with-local-sql-connection with-local-mem-connection with-local-sql-connection*]])
  (:import [eva.error.v1 EvaErrorCode]))

(deftest unit:add-retract
  (with-local-sql-connection conn
    (let [_ @(eva/transact conn [{:db/id (eva/tempid :db.part/db)
                                  :db/ident :my/doc
                                  :db.install/_attribute :db.part/db
                                  :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}])]
      (is (empty? (eva/datoms (eva/db conn) :eavt 0 :my/doc)))
      (let [db (:db-after @(eva/transact conn '[[:db/add 0 :my/doc "Default Partition"]]))]
        (is (= "Default Partition"
               (:v (first (eva/datoms db :eavt 0 :my/doc))))))
      (let [db (:db-after @(eva/transact conn '[[:db/retract 0 :my/doc "Default Partition"]]))]
        (is (empty? (eva/datoms db :eavt 0 :my/doc)))))))

(deftest unit:entity
  (with-local-sql-connection conn
    (let [init-db (eva/db conn)
          part-ent (eva/entity init-db 0)]
      (is (contains? (into {} (eva/touch part-ent)) (eva/ident init-db 17))))))

(deftest unit:attribute-validation
  (with-local-sql-connection conn
    (let [tx1 [{:db.install/_attribute :db.part/db
                :db/id                 (eva/tempid :db.part/db)}]
          tx2 [{:db.install/_partition :db.part/db
                :db/id                 (eva/tempid :db.part/db)}]
          tx3 [{:db.install/_attribute :db.part/db
                :db/id                 (eva/tempid :db.part/db)
                :db/ident              :derp
                :db/cardinality        :db.cardinality/many
                :db/valueType          :db.type/long}]
          tx4 [{:db.install/_attribute :db.part/db
                :db/id                 1024
                :db/ident              :derp
                :db/cardinality        :db.cardinality/many
                :db/valueType          :db.type/string}]]

      (is (thrown-with-msg? Exception
                            #"Cannot install incomplete attribute.*"
                            (let [tx-data (:tx-data @(eva/transact conn tx1))]
                              (println (into [] tx-data)))))
      (is (thrown-with-msg? Exception
                            #"Cannot install incomplete partition.*"
                            @(eva/transact conn tx2)))
      @(eva/transact conn tx3)
      (is (thrown-with-msg? Exception
                            #"Cannot process retraction on already-installed schema"
                            @(eva/transact conn tx4))))))

(deftest unit:unique-mismatching
  (with-local-sql-connection conn
    (let [schema-tx [{:db.install/_attribute :db.part/db
                      :db/id                 (eva/tempid :db.part/db)
                      :db/ident              :unique-attr1
                      :db/cardinality        :db.cardinality/one
                      :db/valueType          :db.type/long
                      :db/unique             :db.unique/identity}
                     {:db.install/_attribute :db.part/db
                      :db/id                 (eva/tempid :db.part/db)
                      :db/ident              :unique-attr2
                      :db/cardinality        :db.cardinality/one
                      :db/valueType          :db.type/string
                      :db/unique             :db.unique/identity}]
          seed-tx [[:db/add (eva/tempid :db.part/user) :unique-attr1 42]
                   [:db/add (eva/tempid :db.part/user) :unique-attr2 "id"]]
          smash-tx [{:db/id        (eva/tempid :db.part/db)
                     :db/doc       "wololo"
                     :unique-attr1 42
                     :unique-attr2 "id"}]]
      @(eva/transact conn schema-tx)
      @(eva/transact conn seed-tx)
      (is (thrown-with-msg? Exception
                            #"Cannot merge two extant permanent ids"
                            @(eva/transact conn smash-tx))))))

(deftest unit:serialize-custom-types
  (with-local-sql-connection conn
    (let [doc-fn (clojure.edn/read-string {:readers *data-readers*}
                                          '"#db/fn {:lang \"clojure\"
                                                  :params [db e doc]
                                                  :code [[:db/add e :db/doc doc]]}")
          db-fn-tx [[:db/add
                     (eva/tempid :db.part/user -1)
                     :db/fn
                     doc-fn]]
          _ (comment
              ;; TODO: re-enable when adding byte-type attrs
              ba-schema [{:db.install/_attribute :db.part/db
                          :db/id                 (eva/tempid :db.part/db)
                          :db/ident              :byte-array-attr
                          :db/cardinality        :db.cardinality/one
                          :db/valueType          :db.type/bytes}]
              ba-tx1 [[:db/add
                       (eva/tempid :db.part/user -1)
                       :byte-array-attr
                     (bytes/bbyte-array (byte-array [1 2 3]))]]
              ba-tx2 [[:db/add
                       (eva/tempid :db.part/user -1)
                       :byte-array-attr
                       (byte-array [1 2 3 4])]])]

      (is @(eva/transact conn db-fn-tx))
      (comment
        ;; TODO: re-enable when adding byte-type attrs
        (is @(eva/transact conn ba-schema))
        (is @(eva/transact conn ba-tx1))
        (is @(eva/transact conn ba-tx2))))))

(deftest unit:built-in-retract-entity
  (with-local-sql-connection conn
    (let [schema [{:db.install/_attribute :db.part/db
                   :db/id (eva/tempid :db.part/db)
                   :db/ident :comp
                   :db/valueType :db.type/ref
                   :db/isComponent true
                   :db/cardinality :db.cardinality/many}]
          tid1 (eva/tempid :db.part/user -1)
          tid2 (eva/tempid :db.part/user -2)
          tx     [{:db/id tid1
                   :db/ident :foo
                   :comp [{:db/ident :bar
                           :comp [{:db/ident :baz
                                   :db/id tid2}]}
                          {:db/ident :bip}]}
                  {:db/id (eva/tempid :db.part/user)
                   :db/ident :yolo
                   :comp [tid1 tid2]}]
          _ (debug "schema:")
          _ @(eva/transact conn schema)
          _ (debug "entity:")
          tres1 @(eva/transact conn tx)
          pid (eva/resolve-tempid (eva/db conn) (:tempids tres1) tid1)
          _ (debug "retract:")
          tres2 @(eva/transact conn [[:db.fn/retractEntity pid]])]
      ;; TODO: something closer to equality testing vs statistical testing
      (is (= {true 1 false 9} (frequencies (map :added (:tx-data tres2))))))))

(deftest unit:built-in-cas
  (with-local-sql-connection conn
    (let [schema [{:db.install/_attribute :db.part/db
                   :db/id (eva/tempid :db.part/db)
                   :db/ident :cmany
                   :db/valueType :db.type/long
                   :db/cardinality :db.cardinality/many}
                  {:db.install/_attribute :db.part/db
                   :db/id (eva/tempid :db.part/db)
                   :db/ident :reference
                   :db/valueType :db.type/ref
                   :db/cardinality :db.cardinality/one}
                  {:db.install/_attribute :db.part/db
                   :db/id (eva/tempid :db.part/db)
                   :db/ident :cone/bool
                   :db/cardinality :db.cardinality/one
                   :db/valueType :db.type/boolean}]
          _ @(eva/transact conn schema)
          tid1 (eva/tempid :db.part/user -1)
          tres1 @(eva/transact conn [[:db/add tid1 :db/doc "1"]])
          pid (eva/resolve-tempid (eva/db conn) (:tempids tres1) tid1)
          tres2 @(eva/transact conn [[:db.fn/cas pid :db/doc "1" "2"]
                                     [:db.fn/cas pid :db/ident nil :foo]])
          tres3 @(eva/transact conn [[:db.fn/cas pid :db/doc "2" nil]])
          tres4 @(eva/transact conn [[:db.fn/cas pid :db/doc nil nil]])

          ;; check that cas works with tempids, lookup refs, and :db/idents
          tid2 (eva/tempid :db.part/user)
          tid3 (eva/tempid :db.part/user)
          tid4 (eva/tempid :db.part/user)

          tres5 @(eva/transact conn [[:db.fn/cas pid :reference nil tid2]
                                     {:db/id tid2
                                      :db/ident :thing1}
                                     {:db/id tid3
                                      :db/ident :thing2}
                                     {:db/id tid4
                                      :db/ident :thing3}])
          [pid2 pid3 pid4] (map (eva/resolve-tempid (:db-after tres5) (:tempids tres5))
                                [tid2 tid3 tid4])
          tres6 @(eva/transact conn [[:db.fn/cas pid :reference :thing1 [:db/ident :thing2]]])
          tres7 @(eva/transact conn [[:db.fn/cas pid :reference [:db/ident :thing2] :thing3]])
          tres8 @(eva/transact conn [[:db.fn/cas pid :reference :thing3 nil]])

          ;; all possible boolean cas-state transitions
          bool-transitions [[nil true]
                            [nil false]
                            [nil nil]
                            [true nil]
                            [true false]
                            [true true]
                            [false nil]
                            [false true]
                            [false false]]]
      (is (= {true 3 false 1} (frequencies (map :added (:tx-data tres2)))))
      (is (= {:db/ident :foo :db/doc "2"}
             (eva/pull (:db-after tres2) [:db/ident :db/doc] pid)))
      (is (thrown? Exception
                   @(eva/transact conn [[:db.fn/cas pid :cmany nil 123]])))
      (is (thrown? Exception
                   @(eva/transact conn [[:db.fn/cas [:db/ident :doesnt-exist] :cmany nil 123]])))
      (is (= {:db/ident :foo}
             (eva/pull (:db-after tres3) [:db/ident :db/doc] pid)
             (eva/pull (:db-after tres4) [:db/ident :db/doc] pid)))

      (is (= {:db/ident :foo :reference {:db/ident :thing1}}
             (eva/pull (:db-after tres5) [:db/ident {:reference [:db/ident]}] pid)))

      (is (= {:db/ident :foo :reference {:db/ident :thing2}}
             (eva/pull (:db-after tres6) [:db/ident {:reference [:db/ident]}] pid)))

      (is (= {:db/ident :foo :reference {:db/ident :thing3}}
             (eva/pull (:db-after tres7) [:db/ident {:reference [:db/ident]}] pid)))

      (is (= {:db/ident :foo}
             (eva/pull (:db-after tres8) [:db/ident {:reference [:db/ident]}] pid)))

      (doseq [[init final] bool-transitions]
        (let [->str #(if (nil? %) "nil" (str %))
              name (keyword (str (->str init) "->" (->str final)))
              tx1 [(cond-> {:db/id (eva/tempid :db.part/user)
                            :db/ident name}
                     (some? init) (assoc :cone/bool init))]
              tx2 [[:db.fn/cas [:db/ident name] :cone/bool init final]]
              tx1-res @(transact conn tx1)
              tx2-res @(transact conn tx2)]
          (is (= (:cone/bool (eva/pull (:db-after tx1-res) [:cone/bool] [:db/ident name]))
                 init))
          (is (= (:cone/bool (eva/pull (:db-after tx2-res) [:cone/bool] [:db/ident name]))
                 final)))))))

(deftest unit:missing?
  (with-local-mem-connection conn
    (let [db (eva/db conn)

          all-idents (eva/q '[:find [?ident ...]
                              :in $
                              :where
                              [?e :db/ident ?ident]] db)

          not-missing (eva/q '[:find [?ident ...]
                               :in $
                               :where
                               [?e :db/ident ?ident]
                               (not [(missing? $ ?e :db/doc)])] db)

          missing-doc (eva/q '[:find [?ident ...]
                               :in $
                               :where
                               [?e :db/ident ?ident]
                               [(missing? $ ?e :db/doc)]] db)]

      (is (= (count all-idents)
             (+ (count not-missing)
                (count missing-doc))))
      (is (= #{:db/fn :db/index :db/unique :db.part/user :db.part/db :db/valueType
               :db/txInstant :db/noHistory :db/isComponent :db/fulltext
               :db.part/tx :db/cardinality :db/doc :db/ident :db.install/valueType
               :db.install/partition :db.install/attribute}
             (set not-missing))))))

(deftest unit:get-else
  (with-local-mem-connection conn
    (let [_ @(eva/transact conn [{:db/id (eva/tempid :db.part/db)
                                  :db/ident :my/doc
                                  :db.install/_attribute :db.part/db
                                  :db/valueType :db.type/string
                                  :db/cardinality :db.cardinality/one}])]
      (is (= '#{("N/A")} (set (eva/q '[:find ?doc :in $ :where
                                       [(get-else $ :db.part/db :my/doc "N/A") ?doc]]
                                     (eva/db conn)))))
      @(eva/transact conn '[[:db/add :db.part/db :my/doc "Doc'd"]])
      (is (= '#{("Doc'd")} (set (eva/q '[:find ?doc :in $ :where
                                         [(get-else $ :db.part/db :my/doc "N/A") ?doc]]
                                       (eva/db conn))))))))

(defn cur-range [window-size tx-num]
  (range (max 0 (inc (- tx-num window-size)))
         (inc tx-num)))

(defn ->ms [x]
  (float (/ x 1e6)))

(deftest unit:db-invoke
  (with-local-mem-connection conn
    (let [pid (first (vals (:tempids @(transact conn [[:db/add (eva/tempid :db.part/user) :db/doc "docstring"]]))))
          db (eva/db conn)]
      (is (= #{[:db/add pid :db/doc "docstring2"]
               [:db/retract pid :db/doc "docstring"]}
             (set (eva/invoke db :db.fn/cas db pid :db/doc "docstring" "docstring2")))))))

(deftest unit:map-type
  (with-local-mem-connection conn
    (= 2 (count (:tx-data @(transact conn [(doto (java.util.HashMap.)
                                             (.put :db/id (eva/tempid :db.part/user))
                                             (.put :db/doc "thing"))]))))))

(deftest unit:confirm-coercion-of-map-keys
  (with-local-mem-connection conn
    (= 5 (count (:tx-data @(transact conn [(doto (java.util.HashMap.)
                                             (.put :db.install/_attribute :db.part/db)
                                             (.put :db/id (eva/tempid :db.part/db))
                                             (.put "db/ident" :new-attr)
                                             (.put ":db/cardinality" :db.cardinality/one)
                                             (.put :db/valueType :db.type/keyword))]))))))

(deftest unit:retract-unique-attribute
  (with-local-mem-connection conn
    (let [_ @(eva/transact conn [{:db/id (eva/tempid :db.part/db)
                                  :db.install/_attribute :db.part/db
                                  :db/valueType :db.type/long
                                  :db/unique :db.unique/identity
                                  :db/ident :my-id
                                  :db/cardinality :db.cardinality/one}])
          pid (-> @(eva/transact conn [{:db/id (eva/tempid :db.part/user)
                                        :my-id 12345678}])
                  :tempids
                  vals
                  first)]
      (is (= 1 (count (eva/q '[:find [?e ...]
                               :where [?e :my-id]] (eva/db conn)))))
      ;; Check that we don't throw.
      (is (= 2 (count (:tx-data @(eva/transact conn [[:db.fn/retractEntity pid]])))))

      (is (empty? (eva/q '[:find [?e ...]
                           :where [?e :my-id]] (eva/db conn)))))))

(deftest unit:transaction-function-exceptions
  (eva.config/with-overrides {:eva.transaction-pipeline.compile-db-fns true}
    (with-local-mem-connection conn
      @(eva/transact conn [{:db/id (eva/tempid :db.part/user)
                            :db/ident :throw?
                            :db/fn (eva/function {:lang   :clojure
                                                  :params '[db throw?]
                                                  :code   '(when throw?
                                                             (throw (ex-info
                                                                     "my custom exception msg"
                                                                     {:exception-type 1234
                                                                      :args [throw?]})))})}])
      (is (thrown-with-msg?
           java.util.concurrent.ExecutionException
           #"Attempted to compile database function and failed"
           (try @(eva/transact conn [{:db/id (eva/tempid :db.part/user)
                                      :db/ident :broken
                                      :db/fn (eva/function {:lang   :clojure
                                                            :params '[db]
                                                            :code   '(not-defined-symbol)})}]))))
      (try @(eva/transact conn [[:throw? true]])
           (catch java.util.concurrent.ExecutionException juce
             (is (= {:exception-type 1234
                     :args [true]}
                    (-> juce
                        (.getCause)
                        (.getCause)
                        (ex-data))))))

      (try @(eva/transact conn [[:throw? true]])
           (catch java.util.concurrent.ExecutionException juce
             (is (= {:exception-type 1234
                     :args [true]}
                    (ex-data (.getCause (.getCause juce)))))

             (is (= "my custom exception msg"
                    (.getMessage (.getCause (.getCause juce))))))))))

(deftest unit:multiple-c1-values-for-entity
  (with-local-mem-connection conn
    (let [_ @(eva/transact conn [{:db/id #db/id [:db.part/db]
                                  :db/ident :system/tx
                                  :db/doc "The latest tx this system was updated at"
                                  :db/valueType :db.type/ref
                                  :db/cardinality :db.cardinality/one
                                  :db.install/_attribute :db.part/db}])
          ;; test the attr with both transaction tempid and non-transaction tempid
          tid1 (eva/tempid :db.part/user)
          tid2 (eva/tempid :db.part/user)

          tids (:tempids @(eva/transact conn [[:db/add tid1 :system/tx (eva/tempid :db.part/tx)]
                                              [:db/add tid2 :system/tx (eva/tempid :db.part/user -1)]
                                              [:db/add (eva/tempid :db.part/user -1) :db/doc "doc1"]]))

          [pid1 pid2] (map (eva/resolve-tempid (eva/db conn) tids) [tid1 tid2])

          _ @(eva/transact conn [[:db/add pid1 :system/tx (eva/tempid :db.part/tx)]
                                 [:db/add pid2 :system/tx (eva/tempid :db.part/user -1)]
                                 [:db/add (eva/tempid :db.part/user -1) :db/doc "doc2"]])]

      (is (= [1 1] (map (comp count (partial eva/q '[:find [?tx ...]
                                                     :in $ ?pid
                                                     :where [?pid :system/tx ?tx]] (eva/db conn))) [pid1 pid2]))))))

(deftest unit:retract-vals
  (with-local-mem-connection conn
    (let [schema [{:db.install/_attribute :db.part/db
                   :db/id #db/id[:db.part/user]
                   :db/ident :numbers
                   :db/valueType :db.type/long
                   :db/cardinality :db.cardinality/many}

                  {:db/id #db/id[:db.part/user]
                   :db/ident :fn/retractAll
                   :db/doc "Retracts all values under the entity-attribute pair."
                   :db/fn
                   #db/fn {:lang "clojure"
                           :params [db eid attr]
                           :code (when-let [eid (eva.api/entid db eid)]
                                   (for [[e a v] (eva.api/datoms db :eavt eid attr)]
                                     [:db/retract e a v]))}}

                  {:db/id #db/id[:db.part/user]
                   :db/ident :fn/retractMany
                   :db/doc "Expands into a :db/retract statement for each v in vs."
                   :db/fn
                   #db/fn {:lang "clojure"
                           :params [db eid attr vs]
                           :code (for [v vs] [:db/retract eid attr v])}}]

          tid (eva/tempid :db.part/db -1)

          data  [{:db/id tid
                  :db/ident :number-holder
                  :numbers (range 10)}]

          _ @(transact conn schema)

          res @(transact conn data)
          db1 (:db-after res)
          pid (eva/resolve-tempid db1 (:tempids res) tid)]
      (is (= (set (range 10))
             (set (:numbers (eva/pull db1 [:numbers] pid)))))

      (let [retract-tx-1 [[:fn/retractMany pid :numbers [0 1 2 3 4]]]
            retract-tx-2 [[:fn/retractAll pid :numbers]]
            db2 (:db-after @(transact conn retract-tx-1))
            db3 (:db-after @(transact conn retract-tx-2))]
        (is (= #{5 6 7 8 9}
               (set (:numbers (eva/pull db2 [:numbers] pid)))))
        (is (= #{}
               (set (:numbers (eva/pull db3 [:numbers] pid)))))))))

(deftest unit:map-entity-missing-id
  (with-local-mem-connection conn
    (is (thrown-with-msg?
         Exception
         #"Map entity forms must contain a :db/id"
         @(transact conn [{:db/ident :foo :db/doc "bar"}])))))

(deftest unit:db-fn-containing-map
  (with-local-mem-connection conn
    ;; the following throws if the map normalization does an arbitrary walk
    (is @(transact conn [{:db/ident :function
                          :db/id (eva/tempid :db.part/user)
                          :db/fn
                          #db/fn
                           {:lang "clojure"
                            :params []
                            :code
                            {:db/id (d/tempid :db.part/user)
                             :db/ident :nested-data}}}]))))

(deftest unit:replace-db-fn
  (with-local-mem-connection conn
    (let [tx1 [{:db/ident :add-doc
                :db/id (eva/tempid :db.part/user)
                :db/fn
                #db/fn
                 {:lang "clojure"
                  :params []
                  :code
                  0}}]
          tx2 [{:db/ident :add-doc
                :db/id (eva/tempid :db.part/user)
                :db/fn
                #db/fn
                 {:lang "clojure"
                  :params [x]
                  :code (+ x 0)}}]
          tx3 [[:db.fn/retractEntity :add-doc]]
          tx4 [{:db/ident :add-doc
                :db/id (eva/tempid :db.part/user)
                :db/fn
                #db/fn
                 {:lang "clojure"
                  :params [x y]
                  :code (+ x y 0)}}]
          db1 (:db-after @(eva/transact conn tx1))
          db2 (:db-after @(eva/transact conn tx2))
          db3 (:db-after @(eva/transact conn tx3))
          db4 (:db-after @(eva/transact conn tx4))]

      (is (= 0 (eva/invoke db1 :add-doc)))
      (is (= 1 (eva/invoke db2 :add-doc 1)))
      (is (thrown? Exception (eva/invoke db3 :add-doc)))
      (is (= 2 (eva/invoke db4 :add-doc 1 1))))))

(deftest unit:historic
  (with-local-mem-connection conn
    (let [tx1 [[:db/add (eva/tempid :db.part/user -1) :db/doc "new!"]
               [:db/add (eva/tempid :db.part/user -1) :db/ident :this-guy]]
          res2 @(eva/transact conn tx1)
          tids (:tempids res2)
          pid (-> tids first val)
          tx2 [[:db.fn/retractEntity pid]]
          res2 @(eva/transact conn tx2)
          tx3 [[:db/add pid :db/doc "new too!"]]
          res3 @(eva/transact conn tx3)
          tx4 [[:db.fn/retractEntity pid]]
          res4 @(eva/transact conn tx4)
          hdb (eva/history (eva/db conn))
          dat-res (map (juxt :e :a :v :tx :added) (eva/datoms hdb :eavt pid))
          q-res (eva/q '[:find ?e ?a ?v ?tx ?added
                         :in $ ?e
                         :where
                         [?e ?a ?v ?tx ?added]]
                       hdb pid)]
      (is (= 2 (count (first (multi-select-datoms-ordered hdb [:avet [:db/ident :this-guy]])))))
      (is (= 6 (count dat-res) (count q-res)))
      (is (= (set dat-res) (set q-res))))))

(deftest unit:user-defined-tx-instant
  (with-local-mem-connection conn
    (let [epoch-plus (java.util.Date. 1)
          tx-data [[:db/add (eva/tempid :db.part/tx) :db/txInstant epoch-plus]]
          res (:tx-data @(transact conn tx-data))]
      (is (= 1 (count res)))
      (is (= epoch-plus (:v (first res)))))))

(deftest unit:cardinality-many-expansion
  (with-local-mem-connection conn
    (let [_ @(transact conn [{:db/id #db/id [:db.part/db]
                              :db/ident :multi/test
                              :db/valueType :db.type/string
                              :db/cardinality :db.cardinality/many
                              :db.install/_attribute :db.part/db}])]
      (are [cnt v]
           (= cnt
              (-> (eva/transact conn [{:db/id (eva/tempid :db.part/user)
                                       :multi/test v}])
                  deref
                  :tx-data
                  count))
        2  "1"
        3 ["a" "b"]
        3 (list "a" "b")
        3 #{"a" "b"}
        3 (doto (java.util.LinkedList.) (.add "a") (.add "b"))
        3 (doto (java.util.HashSet.) (.add "a") (.add "b"))))))

(deftest unit:unknown-attribute-in-map-expansion
  (with-local-mem-connection conn
    (is-thrown? {:msg-re #"This database does not contain the provided attribute."
                 :error-codes #{EvaErrorCode/MODEL_CONSTRAINT_VIOLATION}
                 :eva-code 5000
                 :error-type :attribute-resolution/unresolvable-attribute
                 :unwrapper (memfn ^Throwable getCause)}
                (try
                  @(transact conn
                             [{:db/id #db/id[:db.part/user -1],
                               :autho2r/name "U189761514X2175691368"}])))))
