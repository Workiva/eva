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

(ns eva.transaction-pipeline.errors-test
  (:require [eva.api :as eva]
            [eva.v2.server.transactor-test-utils :refer [with-local-mem-connection]]
            [eva.error :as err]
            [clojure.test :refer :all])
  (:import (java.util.concurrent ExecutionException)
           (eva.error.v1 EvaErrorCode)))

(defn test-transaction-pipeline-exceptions [conn]
  (let [schema [{:db.install/_attribute :db.part/db
                 :db/ident :unique/identity
                 :db/unique :db.unique/identity
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/keyword
                 :db/id (eva/tempid :db.part/db)}
                {:db.install/_attribute :db.part/db
                 :db/ident :unique/value
                 :db/unique :db.unique/value
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/keyword
                 :db/id (eva/tempid :db.part/db)}
                {:db.install/_attribute :db.part/db
                 :db/ident :component.card-one/ref
                 :db/isComponent true
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/ref
                 :db/id (eva/tempid :db.part/db)}
                {:db.install/_attribute :db.part/db
                 :db/ident :component.card-many/ref
                 :db/isComponent true
                 :db/cardinality :db.cardinality/many
                 :db/valueType :db.type/ref
                 :db/id (eva/tempid :db.part/db)}
                {:db.install/_attribute :db.part/db
                 :db/ident :card-one/ref
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/ref
                 :db/id (eva/tempid :db.part/db)}
                {:db.install/_attribute :db.part/db
                 :db/ident :card-many/ref
                 :db/cardinality :db.cardinality/many
                 :db/valueType :db.type/ref
                 :db/id (eva/tempid :db.part/db)}
                {:db/id #db/id [:db.part/db]
                 :db/ident :double/many
                 :db/valueType :db.type/double
                 :db/cardinality :db.cardinality/many
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id [:db.part/db]
                 :db/ident :double/one
                 :db/valueType :db.type/double
                 :db/cardinality :db.cardinality/one
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id [:db.part/db]
                 :db/ident :string/one
                 :db/valueType :db.type/string
                 :db/cardinality :db.cardinality/one
                 :db.install/_attribute :db.part/db}
                ;; TODO: re-enable when adding byte-type attrs
                #_{:db/id #db/id [:db.part/db]
                 :db/ident :bytes/one
                 :db/valueType :db.type/bytes
                 :db/cardinality :db.cardinality/one
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id [:db.part/db]
                 :db/ident :float/many
                 :db/valueType :db.type/float
                 :db/cardinality :db.cardinality/many
                 :db.install/_attribute :db.part/db}
                {:db/id #db/id [:db.part/db]
                 :db/ident :float/one
                 :db/valueType :db.type/float
                 :db/cardinality :db.cardinality/one
                 :db.install/_attribute :db.part/db}]
        seed [[:db/add (eva/tempid :db.part/user) :unique/value :extant-uv]]
        _ @(eva/transact conn schema)
        init-db (:db-after @(eva/transact conn seed))
        unwrapper (fn [e]
                    (is (instance? ExecutionException e))
                    (.getCause ^Exception e))
        test-fn (fn [tx-data msg-regex error-codes code http]
                  (testing (format "tx-data: %s" (str tx-data))
                    (err/is-thrown?
                     {:msg-re msg-regex
                      :unwrapper unwrapper
                      :error-codes error-codes
                      :eva-code code
                      :http-code http}
                     @(eva/transact conn tx-data))))
        incorrect-transact-syntax #{EvaErrorCode/INCORRECT_TRANSACT_SYNTAX
                                    EvaErrorCode/INCORRECT_SYNTAX
                                    EvaErrorCode/API_ERROR}
        model-constraint-violation #{EvaErrorCode/MODEL_CONSTRAINT_VIOLATION,
                                     EvaErrorCode/TRANSACTION_PIPELINE_FAILURE,
                                     EvaErrorCode/PROCESSING_FAILURE}
        nyi #{EvaErrorCode/METHOD_NOT_YET_IMPLEMENTED
              EvaErrorCode/API_ERROR}]
    (testing "transaction syntax and semantics"
      (test-fn [123]
               #"Cannot interpret object as a transaction command: received invalid object. Expected a List command or a Map entity."
               incorrect-transact-syntax
               3000
               400)
      (test-fn [[:db.doesnt/exist]]
               #"Cannot interpret object as a transaction command: received list, but its first element should be :db/add, :db/retract, or a transaction function."
               incorrect-transact-syntax
               3000
               400)
      (test-fn [[123]]
               #"Cannot interpret object as a transaction command: received list, but its first element should be :db/add, :db/retract, or a transaction function."
               incorrect-transact-syntax
               3000
               400)
      (test-fn [[:db/add :foo]]
               #":db/add command contains 1 arg\(s\), expected 3"
               incorrect-transact-syntax
               3000
               400)
      (test-fn [[":db/retract" 1 2 3 4]]
               #":db/retract command contains 4 arg\(s\), expected 3"
               incorrect-transact-syntax
               3000
               400)
      (test-fn [[:db/add nil 2 3]]
               #":db/add contains nil entity id"
               incorrect-transact-syntax
               3000
               400)
      (test-fn [[":db/add" 1 nil 3]]
               #":db/add contains nil attribute"
               incorrect-transact-syntax
               3000
               400)
      (test-fn [[:db/retract 1 2 nil]]
               #":db/retract contains nil value"
               incorrect-transact-syntax
               3000
               400)
      (test-fn [[:db/add :db.part/db :db/doc :not-a-string]]
               #"Wrong value type for attribute"
               model-constraint-violation
               5000
               422)
      (test-fn [[:db/add 123456789 :db/doc "this entity doesn't exist."]]
               #"The entity id 123456789 has not been allocated in this database"
               model-constraint-violation
               5000
               422)
      (test-fn [[:db/add 0 :card-one/ref 123456789]]
               #"The entity id 123456789 has not been allocated in this database"
               model-constraint-violation
               5000
               422))
    (testing "map entity semantics exceptions"
      (test-fn [{:db/ident :foo}]
               #"Map entity forms must contain a :db/id"
               incorrect-transact-syntax
               3000
               400)
      (test-fn [{:db/id (eva/tempid :db.part/user)}]
               #"Map entity forms must contain attributes other than :db/id"
               incorrect-transact-syntax
               3000
               400)
      (test-fn [{:db/id (eva/tempid :db.part/user)
                 :card-one/ref {:db/doc "dangling attribute"}}]
               #"Nested entity maps must be under a component attribute, contain a unique attribute, or have an explicit :db/id"
               incorrect-transact-syntax
               3000
               400)
      (test-fn [{:db/id (eva/tempid :db.part/user)
                 :card-many/ref [{:db/doc "dangling attribute"}]}]
               #"Nested entity maps must be under a component attribute, contain a unique attribute, or have an explicit :db/id"
               incorrect-transact-syntax
               3000
               400))
    (testing "schema attribute properties exceptions"
      (test-fn [[:db/add 0 :db/doc "a"]
                [:db/add 0 :db/doc "b"]]
               #"Cannot add multiple values for cardinality one attribute"
               model-constraint-violation
               5000
               422)
      (test-fn [[:db/retract :db/ident :db/cardinality :db.cardinality/one]]
               #"Cannot process retraction on already-installed schema"
               model-constraint-violation
               5000
               422)
      (test-fn [[:db/add (eva/tempid :db.part/user) :db/cardinality :db.cardinality/one]]
               #"Cannot process command .* without corresponding :db.install/attribute"
               model-constraint-violation
               5000
               422)
      (test-fn [[:db/add (eva/tempid :db.part/user) :db/ident :db/foo]]
               #"Cannot process command .* The 'db' namespace idents are reserved and cannot be modified."
               model-constraint-violation
               5000
               422)
      (test-fn [[:db/add 0 :db.install/attribute (eva/tempid :db.part/user)]]
               #"Cannot install incomplete attribute; missing required schema attributes"
               model-constraint-violation
               5000
               422)
      (test-fn [[:db/add 0 :db.install/partition (eva/tempid :db.part/user)]]
               #"Cannot install incomplete partition; missing required attributes"
               model-constraint-violation
               5000
               422)
      (test-fn [{:db.install/_attribute :db.part/db
                 :db/ident :noHist
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/long
                 :db/id (eva/tempid :db.part/db)
                 :db/noHistory true}]
               #"The :db/noHistory attribute property is not yet implemented"
               nyi
               3999
               501)
      (test-fn [{:db.install/_attribute :db.part/db
                 :db/ident :fulltext
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/string
                 :db/id (eva/tempid :db.part/db)
                 :db/fulltext true}]
               #"The :db/fulltext attribute property is not yet implemented"
               nyi
               3999
               501)
      (test-fn [{:db.install/_valueType :db.part/db
                 :db/id (eva/tempid :db.part/db)}]
               #"The :db.install/valueType attribute is not yet implemented"
               nyi
               3999
               501))
    (testing "uniqueness properties"
      (test-fn [{:db/id (eva/tempid :db.part/user -1)
                 :db/ident :foo}
                {:db/id (eva/tempid :db.part/user -2)
                 :db/ident :foo}]
               #"Cannot merge two differently-tagged user-ids. This typically happens when two distinct entity-ids either are explicitly or implicitly assigned the same :db/unique attribute-value pair."
               model-constraint-violation
               5000
               422)
      (test-fn [{:db/id 1
                 :db/ident :foo}
                {:db/id 0
                 :db/ident :foo}]
               #"Cannot merge two extant permanent ids. This typically happens when two distinct entity-ids either are explicitly or implicitly assigned the same :db/unique attribute-value pair."
               model-constraint-violation
               5000
               422)
      ;; TODO: re-enable when adding byte-type attrs
      #_(test-fn [{:db.install/_attribute :db.part/db
                 :db/ident :unique/bytes
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/bytes
                 :db/unique :db.unique/identity
                 :db/id (eva/tempid :db.part/db)}]
               #"Cannot install unique byte-typed attributes"
               model-constraint-violation
               5000
               422)
      (test-fn [{:db.install/_attribute :db.part/db
                 :db/ident :unique/cmany
                 :db/cardinality :db.cardinality/many
                 :db/valueType :db.type/long
                 :db/unique :db.unique/identity
                 :db/id (eva/tempid :db.part/db)}]
               #"Cannot install cardinality-many unique attributes"
               model-constraint-violation
               5000
               422)
      (test-fn [[:db/add (eva/tempid :db.part/user) :unique/value :extant-uv]]
               #"Cannot assert a new temp-id with the same :db.unique/value attribute-value pair as an extant eid"
               model-constraint-violation
               5000
               422))
    (testing "attribute naming"
      (test-fn [{:db.install/_attribute :db.part/db
                 :db/ident :_underscore
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/long
                 :db/id (eva/tempid :db.part/db)}]
               #"Cannot install attribute with an ident name that has a leading underscore"
               incorrect-transact-syntax
               3000
               400)
      (test-fn [{:db.install/_attribute :db.part/db
                 :db/ident :leading/_underscore
                 :db/cardinality :db.cardinality/one
                 :db/valueType :db.type/long
                 :db/id (eva/tempid :db.part/db)}]
               #"Cannot install attribute with an ident name that has a leading underscore"
               incorrect-transact-syntax
               3000
               400))

    (testing "terrible double and float values are disallowed"
      (test-fn [[:db/add (eva/tempid :db.part/user) :double/many Double/NaN]]
               #"Wrong value type for attribute"
               model-constraint-violation
               5000
               422)
      (test-fn [[:db/add (eva/tempid :db.part/user) :double/one Double/NaN]]
               #"Wrong value type for attribute"
               model-constraint-violation
               5000
               422)
      (test-fn [[:db/add (eva/tempid :db.part/user) :float/many Float/NaN]]
               #"Wrong value type for attribute"
               model-constraint-violation
               5000
               422)
      (test-fn [[:db/add (eva/tempid :db.part/user) :float/one Float/NaN]]
               #"Wrong value type for attribute"
               model-constraint-violation
               5000
               422))
    (eva.config/with-overrides {:eva.transaction-pipeline.limit-byte-sizes true}
      ;; TODO: re-enable when adding byte-type attrs
      #_(testing "transaction of bytes attributes are rejected over a certain size"
        (test-fn [[:db/add (eva/tempid :db.part/user) :bytes/one (byte-array 2048 (byte 0x0))]]
                 #"Transaction contains a :db.type/bytes value that is 2048 bytes, but that attribute limits values to 1024 bytes."
                 model-constraint-violation
                 5000
                 422))
      (testing "transaction of string attributes are rejected over a certain length"
       (test-fn [[:db/add (eva/tempid :db.part/user) :string/one (apply str (repeat 2048 \c))]]
                #"Transaction contains a :db.type/string value that is 2048 bytes, but that attribute limits values to 1024 bytes."
                model-constraint-violation
                5000
                422)))))

(deftest unit:v2-mem-pipeline-exceptions
  (with-local-mem-connection conn
    (test-transaction-pipeline-exceptions conn)))
