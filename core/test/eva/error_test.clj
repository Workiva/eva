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

(ns eva.error-test
  (:require [clojure.test :refer :all]
            [eva.api :as eva]
            [eva.error :as err]
            [recide.utils :as ru]
            [recide.sanex :as sanex])
  (:import [eva.error.v1 SanitizedEvaException EvaException]))

(defn produce-exceptions
  [conn]
  (let [schema [{:db/id                 #db/id[:db.part/db]
                 :db/ident              :book/title
                 :db/doc                "Title of a book"
                 :db/valueType          :db.type/string
                 :db/cardinality        :db.cardinality/one
                 :db.install/_attribute :db.part/db}]
        _ @(eva/transact conn schema)
        _ @(eva/transact conn [[:db/add (eva/tempid :db.part/user) :book/title "first book"]])]
    (try @(eva/transact conn [[:db.fn/cas 8796093023233 :book/title "1" "2"]])
         (catch Exception e (.getCause e)))))

(deftest test:eva-exception-specific-test
  (let [conn (eva/connect {:local true})]
    (try
      (binding [sanex/*sanitization-level* sanex/default-sanitization]
        (let [e (produce-exceptions conn)]
          (testing "roundtripping nested exceptions"
            (-> e ru/serialize-throwable ru/deserialize-throwable)
            (is true)) ;; no exceptions were thrown.
          (testing "We throw unsanitized exceptions by default"
            (is (and (instance? EvaException e)
                     (not (instance? SanitizedEvaException e)))))
          (testing "We can produce sanitized exceptions from unsanitized exceptions"
            (let [sanitized (.getSanitized ^EvaException e sanex/*sanitization-level*)]
              (is (instance? SanitizedEvaException sanitized))
              (is (instance? SanitizedEvaException (.getCause ^Exception sanitized))
                  "Recursive sanitization work."))))
        (alter-var-root #'err/*always-sanitize-exceptions* (constantly true))
        (binding [err/*always-sanitize-exceptions* true]
          (let [e (produce-exceptions conn)]
            (testing "The flag causes sanitized exceptions to be produced"
              (is (instance? SanitizedEvaException e)))
            (testing "you can get the unsanitized back out"
              (let [e-orig (.getUnsanitized ^SanitizedEvaException e)]
                (is (and (instance? EvaException e-orig)
                         (not (instance? SanitizedEvaException e-orig)))))))))
      (finally (.release conn)
               (alter-var-root #'err/*always-sanitize-exceptions* (constantly false))))))
