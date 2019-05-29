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

(ns eva.entity-test
  (:require [clojure.test :refer :all]
            [eva.api :as eva]
            [eva.entity :as entity]
            [eva.v2.server.transactor-test-utils :refer [with-local-mem-connection
                                                         with-local-sql-connection]]))

(deftest test:read-keyword
  (are [x y]
       (= x (entity/read-keyword y))
    :foo :foo
    :bar ":bar"
    :foo/bar :foo/bar
    :foo/bar ":foo/bar")

  (are [x]
       (thrown? IllegalArgumentException (entity/read-keyword x))

    nil
    "1" 1
    'foo "'foo"
    {:foo :bar} "{:foo :bar}"))

(deftest test:reverse-ref?
  (are [res attr]
       (= res (entity/reverse-ref? attr))

    false :foo/bar
    true :foo/_bar
    false ":foo/bar"
    true ":foo/_bar")

  (are [bad-attr]
       (thrown? IllegalArgumentException (entity/reverse-ref? bad-attr))
    nil
    "foo"
    123
    123M
    {}
    []
    #{}))

(deftest test:reverse-ref
  (are [result attr]
       (= result (entity/reverse-ref attr))

    :foo/bar :foo/_bar
    :foo/_bar :foo/bar))

(def test-schema [{:db/id                 (eva/tempid :db.part/db)
                   :db/ident              :person/given-name
                   :db/valueType          :db.type/string
                   :db/cardinality        :db.cardinality/one
                   :db.install/_attribute :db.part/db}
                  {:db/id                 (eva/tempid :db.part/db)
                   :db/ident              :person/family-name
                   :db/valueType          :db.type/string
                   :db/cardinality        :db.cardinality/one
                   :db.install/_attribute :db.part/db}
                  {:db/id                 (eva/tempid :db.part/db)
                   :db/ident              :person/child
                   :db/valueType          :db.type/ref
                   :db/cardinality        :db.cardinality/many
                   :db/isComponent        true
                   :db.install/_attribute :db.part/db}])

(defn test-entity-api [conn]
  (let [{db0 :db-after} @(eva/transact conn test-schema)
        {db1 :db-after} @(eva/transact conn [{:db/id              (eva/tempid :db.part/user)
                                              :person/given-name  "John"
                                              :person/family-name "Doe"
                                              :person/child       [{:person/given-name  "Jane"
                                                                    :person/family-name "Doe"}
                                                                   {:person/given-name  "Jimmy"
                                                                    :person/family-name "Doe"}]}])
        john-eid (eva/q '[:find ?e . :where [?e :person/given-name "John"] [?e :person/family-name "Doe"]] db1)]
    (is (some? john-eid))
    (testing "accessing properties on un-touched entity"
      (let [john (eva/entity db1 john-eid)]
        (is (instance? eva.entity.Entity john))
        (is (false? (entity/entity-touched? john)))
        (is (= "John" (:person/given-name john)))
        (is (= "Doe" (:person/family-name john)))
        (is (= 2 (count (:person/child john))))))
    (testing "accessing properties on touched entity"
      (let [john (eva/touch (eva/entity db1 john-eid))]
        (is (instance? eva.entity.Entity john))
        (is (true? (entity/entity-touched? john)))
        (is (= "John" (:person/given-name john)))
        (is (= "Doe" (:person/family-name john)))
        (is (= 2 (count (:person/child john))))
        (is (= #{["Jane" "Doe"] ["Jimmy" "Doe"]}
               (into #{} (map (juxt :person/given-name :person/family-name))
                     (:person/child john))))))))

(deftest test:entity-api
  (testing "in-memory"
    (with-local-mem-connection conn
      (test-entity-api conn)))
  (testing "local-storage"
    (with-local-sql-connection conn
      (test-entity-api conn))))
