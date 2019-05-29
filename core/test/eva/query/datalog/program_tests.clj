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

(ns eva.query.datalog.program-tests
  (:require [eva.query.datalog.protocols :refer :all]
            [eva.query.datalog.program :refer :all]
            [eva.query.datalog.rule :as rule]
            [eva.query.datalog.predicate :as pred]
            [clojure.test :refer :all]))

(deftest unit:query.datalog:program:construction-errors
  ;; non-nil, non-map edbs:
  (is (thrown? Throwable
               (program :edbs [])))
  ;; edbs is map symbol->integers:
  (is (thrown? Throwable
               (program :edbs {'foo 2, 'bar 3})))
  ;; edbs is map integer->EDB
  (is (thrown? Throwable
               (program :edbs {3 (reify EDB), 2 (reify EDB)})))
  ;; non-nil, non-map evaluators:
  (is (thrown? Throwable
               (program :evaluators [])))
  ;; evaluators is a map symbol->integers:
  (is (thrown? Throwable
               (program :evaluators {'foo 2, 'bar 3})))
  ;; evaluators is a map integer->Evaluable:
  (is (thrown? Throwable
               (program :evaluators {3 (reify Evaluable), 2 (reify Evaluable)})))
  ;; rules is a collection of integers:
  (is (thrown? Throwable
               (program :rules [1 2 3 4 5])))
  ;; rules contain extensional predicates with no corresponding entry in edbs:
  (is (thrown? Throwable
               (program :rules [(rule/rule ((pred/rule-predicate 'foo {:term-count 1}) '[?x])
                                           [((pred/extensional-predicate 'bar {:term-count 1}) '[?x])
                                            ((pred/extensional-predicate 'baz {:term-count 1}) '[?x])])]
                        :edbs {'bar (reify EDB)})))
  (is (thrown? Throwable
               (program :rules [(rule/rule ((pred/rule-predicate 'foo {:term-count 1}) '[?x])
                                           [((pred/extensional-predicate 'bar {:term-count 1}) '[?x])
                                            (pred/extensional-predicate 'baz {:term-count 1}) '[?x]])]
                        :edbs {'baz (reify EDB)})))
  ;; rules contain eval-predicates with no corresponding entry in evaluators:
  (is (thrown? Throwable
               (program :rules [(rule/rule ((pred/rule-predicate 'foo {:term-count 1}) '[?x])
                                           [((pred/evaluable-predicate 'bar {:term-count 1}) '[?x])
                                            ((pred/evaluable-predicate 'baz {:term-count 1}) '[?x])])]
                        :evaluators {'bar (reify Evaluable)})))
  (is (thrown? Throwable
               (program :rules [(rule/rule ((pred/rule-predicate 'foo {:term-count 1}) '[?x])
                                           [((pred/evaluable-predicate 'bar {:term-count 1}) '[?x])
                                            ((pred/evaluable-predicate 'baz {:term-count 1}) '[?x])])]
                        :evaluators {'baz (reify Evaluable)})))
  ;; rules contain rules-predicates that don't have matching consequents in the rules themselves.
  (is (thrown? Throwable
               (program :rules [(rule/rule ((pred/rule-predicate 'foo {:term-count 1}) '[?x])
                                           [((pred/rule-predicate 'bar {:term-count 1}) '[?x])
                                            ((pred/evaluable-predicate 'baz {:term-count 1}) '[?x])])]
                        :evaluators {'baz (reify Evaluable)})))
  (is (thrown? Throwable
               (program :rules [(rule/rule ((pred/rule-predicate 'foo {:term-count 2}) '[?x ?y])
                                           [((pred/rule-predicate 'bar {:term-count 1}) '[?z])
                                            ((pred/rule-predicate 'foo {:term-count 2}) '[?z ?y])])]))))
