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

(ns eva.query.datalog.predicate-tests
  (:require [eva.query.datalog.predicate :refer :all]
            [eva.query.datalog.protocols :refer :all :as prot]
            [eva.utils.testing :refer [every-test]]
            [clojure.test :refer :all]))

(deftest unit:query.datalog:predicate:construction-errors
  ;; Tagging it :+ and :-
  (is (thrown? Throwable
               ((predicate 'george {:term-count 2, :type :extensional}) '[?lucas ?star] :+ :-)))
  ;; Tagging both :negative and :+
  (is (thrown? Throwable
               ((predicate 'my {:term-count 3, :type :extensional}) '[?tv ?comes ?tomorrow] :negative :+)))
  ;; Tagging an "extensional-predicate" as :evaluable
  (is (thrown? Throwable
               ((extensional-predicate 'timothy {:term-count 3, :type :evaluable}) '[?dean ?is ?sleepy])))
  ;; Tagging a "rule-predicate" as :extensional
  (is (thrown? Throwable
               ((rule-predicate 'wars {:term-count 2, :type :extensional}) '[?luke ?leia] :-)))
  ;; The terms of the predicate must be a collection, not a single lvar.
  (is (thrown? Throwable
               ((rule-predicate 'i {:term-count 1}) '?shouldnt)))
  ;; What kind of predicate?
  (is (thrown? Throwable
               ((predicate 'kalispell {:term-count 5})
                '[?creamery ?makes ?good ?greek ?yoghurt])))
  ;; Required binding out of range.
  (is (thrown? Throwable
               (rule-predicate 'blah {:term-count 3, :required [0 3]})))
  ;; Required bindings formatting error
  (is (thrown? Throwable
               (rule-predicate 'blah {:term-count 3, :required '[?x ?z]}))))

(defn- type->type?
  [target kw fn]
  (if (= target kw)
    fn
    (complement fn)))

(defn- satisfies-protocol?
  [predicates {target-sym :symbol, target-terms :terms, target-type :type, target-neg :neg?}]
  (let [jvm-type-test (partial satisfies? prot/Predicate)
        symbol-test #(= target-sym (sym %))
        terms-test #(= target-terms (terms %))
        negation-test (if target-neg negated? (complement negated?))
        extensional-test (type->type? target-type :extensional extensional?)
        rule-test (type->type? target-type :rule rule?)
        evaluable-test (type->type? target-type :evaluable evaluable?)
        whole-test (every-test jvm-type-test symbol-test terms-test negation-test extensional-test rule-test evaluable-test)]
    (every? whole-test predicates)))

(deftest unit:query.datalog:predicate:construction
  ;;; positive extensional predicate
  (let [duration (extensional-predicate 'duration {:term-count 3})
        duration-2 (predicate 'duration {:term-count 3, :type :extensional})
        pos-pred-1 (duration '[?spell ?unit ?value])
        pos-pred-2 (duration '[?spell ?unit ?value] :+)
        pos-pred-3 (duration-2 '[?spell ?unit ?value] :+)
        pos-pred-4 (duration-2 '[?spell ?unit ?value])
        pos-pred-5 (map->Predicate {:symbol   'duration,
                                    :terms    '[?spell ?unit ?value],
                                    :-?       false,
                                    :type     :extensional,
                                    :required []})
        pos-preds [pos-pred-1 pos-pred-2 pos-pred-3 pos-pred-4 pos-pred-5]]
    (is (apply = pos-preds)
        "Not all positive extensional predicates are equal.")
    (is (satisfies-protocol? pos-preds
                             {:symbol 'duration, :terms '[?spell ?unit ?value],
                              :type   :extensional, :neg? false})
        "Not all positive extensional predicates implement the predicate protocol correctly."))
  ;; positive rule predicate
  (let [duration (rule-predicate 'duration {:term-count 3})
        duration-2 (predicate 'duration {:term-count 3, :type :rule})
        pos-pred-1 (duration '[?spell ?unit ?value])
        pos-pred-2 (duration '[?spell ?unit ?value] :+)
        pos-pred-3 (duration-2 '[?spell ?unit ?value] :+)
        pos-pred-4 (duration-2 '[?spell ?unit ?value])
        pos-pred-5 (map->Predicate {:symbol   'duration,
                                    :terms    '[?spell ?unit ?value],
                                    :-?       false,
                                    :type     :rule,
                                    :required []})
        pos-preds [pos-pred-1 pos-pred-2 pos-pred-3 pos-pred-4 pos-pred-5]]
    (is (apply = pos-preds)
        "Not all positive rule predicates are equal.")
    (is (satisfies-protocol? pos-preds
                             {:symbol 'duration, :terms '[?spell ?unit ?value],
                              :type   :rule, :neg? false})
        "Not all positive rule predicates implement the predicate protocol correctly."))
  ;;; positive evaluable predicate
  (let [duration (evaluable-predicate 'duration {:term-count 3})
        duration-2 (predicate 'duration {:term-count 3, :type :evaluable})
        pos-pred-1 (duration '[?spell ?unit ?value])
        pos-pred-2 (duration '[?spell ?unit ?value] :+)
        pos-pred-3 (duration-2 '[?spell ?unit ?value] :+)
        pos-pred-4 (duration-2 '[?spell ?unit ?value])
        pos-pred-5 (map->Predicate {:symbol   'duration,
                                    :terms    '[?spell ?unit ?value],
                                    :-?       false,
                                    :type     :evaluable,
                                    :required []})
        pos-preds [pos-pred-1 pos-pred-2 pos-pred-3 pos-pred-4 pos-pred-5]]
    (is (apply = pos-preds)
        "Not all positive evaluable predicates are equal.")
    (is (satisfies-protocol? pos-preds
                             {:symbol 'duration, :terms '[?spell ?unit ?value],
                              :type   :evaluable, :neg? false})
        "Not all positive evaluable predicates implement the predicate protocol correctly."))
  ;; negative extensional predicate
  (let [duration (extensional-predicate 'duration {:term-count 3})
        duration-2 (predicate 'duration {:term-count 3, :type :extensional})
        neg-pred-1 (duration '[?spell ?unit ?value] :-)
        neg-pred-2 (duration '[?spell ?unit ?value] :negative)
        neg-pred-3 (duration-2 '[?spell ?unit ?value] :-)
        neg-pred-4 (duration-2 '[?spell ?unit ?value] :negative)
        neg-pred-5 (map->Predicate {:symbol   'duration,
                                    :terms    '[?spell ?unit ?value],
                                    :-?       true,
                                    :type     :extensional,
                                    :required []})
        neg-preds [neg-pred-1 neg-pred-2 neg-pred-3 neg-pred-4 neg-pred-5]]
    (is (apply = neg-preds)
        "Not all negative extensional predicates are equal.")
    (is (satisfies-protocol? neg-preds
                             {:symbol 'duration, :terms '[?spell ?unit ?value],
                              :type   :extensional, :neg? true})
        "Not all negative extensional predicates implement the predicate protocol correctly."))
  ;; negative rule predicate
  (let [duration (rule-predicate 'duration {:term-count 3})
        duration-2 (predicate 'duration {:term-count 3, :type :rule})
        neg-pred-1 (duration '[?spell ?unit ?value] :-)
        neg-pred-2 (duration '[?spell ?unit ?value] :negative)
        neg-pred-3 (duration-2 '[?spell ?unit ?value] :-)
        neg-pred-4 (duration-2 '[?spell ?unit ?value] :negative)
        neg-pred-5 (map->Predicate {:symbol   'duration,
                                    :terms    '[?spell ?unit ?value],
                                    :-?       true,
                                    :type     :rule,
                                    :required []})
        neg-preds [neg-pred-1 neg-pred-2 neg-pred-3 neg-pred-4 neg-pred-5]]
    (is (apply = neg-preds)
        "Not all negative rule predicates are equal.")
    (is (satisfies-protocol? neg-preds
                             {:symbol 'duration, :terms '[?spell ?unit ?value],
                              :type   :rule, :neg? true})
        "Not all negative rule predicates implement the predicate protocol correctly."))
  ;; negative evaluable predicate
  (let [duration (evaluable-predicate 'duration {:term-count 3})
        duration-2 (predicate 'duration {:term-count 3, :type :evaluable})
        neg-pred-1 (duration '[?spell ?unit ?value] :-)
        neg-pred-2 (duration '[?spell ?unit ?value] :negative)
        neg-pred-3 (duration-2 '[?spell ?unit ?value] :-)
        neg-pred-4 (duration-2 '[?spell ?unit ?value] :negative)
        neg-pred-5 (map->Predicate {:symbol   'duration,
                                    :terms    '[?spell ?unit ?value],
                                    :-?       true,
                                    :type     :evaluable,
                                    :required []})
        neg-preds [neg-pred-1 neg-pred-2 neg-pred-3 neg-pred-4 neg-pred-5]]
    (is (apply = neg-preds)
        "Not all negative evaluable predicates are equal.")
    (is (satisfies-protocol? neg-preds
                             {:symbol 'duration, :terms '[?spell ?unit ?value],
                              :type   :evaluable, :neg? true})
        "Not all negative evaluable predicates implement the predicate protocol correctly.")))
