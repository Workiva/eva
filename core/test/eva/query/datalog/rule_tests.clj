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

(ns eva.query.datalog.rule-tests
  (:require [eva.query.datalog.protocols :refer :all :as prot]
            [eva.query.datalog.predicate :refer :all]
            [eva.query.datalog.rule :refer :all]
            [eva.query.datalog.qsqr.state :as state]
            [clojure.core.unify :as u]
            [eva.utils.testing :refer [every-test]]
            [clojure.test :refer :all]))

(deftest unit:query:datalog:rule:construction-errors
  (let [blah (rule-predicate 'blah {:term-count 3})
        blub (extensional-predicate 'blub {:term-count 3})]
    ;; Consequent isn't a predicate:
    (is (thrown? Throwable
                 (rule '(something something something)
                       [(blah '[?x ?y ?z])])))
    ;; Consequent isn't a rule predicate:
    (is (thrown? Throwable
                 (rule (blub '[?x ?y ?z])
                       [(blah '[?x ?y ?z])])))
    ;; "Antecedents" is plural:
    (is (thrown? Throwable
                 (rule (blah '[?x ?y ?z])
                       (blah '[?x ?y ?z]))))
    ;; Not all antecedents are predicates:
    (is (thrown? Throwable
                 (rule (blah '[?x ?y ?z])
                       [(blah '[?z ?z 2])
                        '(blah [?y ?z ?x])])))
    ;; The consequent is negative:
    (is (thrown? Throwable
                 (rule (blah '[?x ?y ?z] :-)
                       [(blah '[?x ?x ?x])
                        (blah '[?x ?y ?z])])))
    ;; All antecedents are negated:
    (is (thrown? Throwable
                 (rule (blah '[?x ?y ?z])
                       [(blah '[?x ?y ?z] :negative)])))
    ;; unbound lvars:
    (is (thrown? Throwable
                 (rule (blah '[?x ?y ?z])
                       [(blub '[?x ?x ?z])])))))

(defn- satisfies-protocol?
  [rules]
  (let [rule? (partial satisfies? prot/Rule)
        predicates? ((fn [pred?] (every-pred (comp pred? consequent)
                                             (comp (partial every? pred?) antecedents)))
                     (partial satisfies? prot/Predicate))
        positive-consequent? (comp (complement negated?) consequent)
        freshens-and-substs? (fn [rule] ;; TODO: X Y solution?
                               (let [fresh (freshen rule)
                                     mgu (u/unify rule fresh)]
                                 (and (not= rule fresh) ;; freshen does change something
                                      (some? mgu) ;; the results can unify
                                      (= fresh (u/subst rule mgu)) ;; and the unification isn't multi-step
                                      (= (u/subst rule mgu) ;; subst substs
                                         (subst rule mgu)))))
        full-test (every-test rule? predicates? positive-consequent? freshens-and-substs?)]
    (every? full-test rules)))

(deftest unit:query:datalog:rule:construction
  (let [ancestor (predicate 'ancestor {:term-count 2, :type :rule})
        parent (predicate 'parent {:term-count 2, :type :extensional})
        rule-1 (rule (ancestor '[?x ?y])
                     [(ancestor '[?x ?z])
                      (parent '[?z ?y])])
        rule-2 (->Rule [(ancestor '[?x ?z])
                        (parent '[?z ?y])]
                       (ancestor '[?x ?y]))
        all-rules [rule-1 rule-2]]
    (is (apply = all-rules)
        (str (count all-rules) " rules that should be all equal are not all equal.."))
    (is (satisfies-protocol? all-rules)
        "There is an error in the implementation of the Rule protocol.")))

(deftest unit:query:datalog:rule:freshen-expressions
  (let [consequent ((rule-predicate 'compatible {:term-count 2}) '[?a ?b])
        antecedents [((extensional-predicate 'likes {:term-count 2}) '[?a ?x])
                     ((extensional-predicate 'likes {:term-count 2}) '[?b ?x])]
        rule (rule consequent antecedents)
        query (-> ((rule-predicate 'compatible {:term-count 2}) '[Houston ?some-var])
                  (state/predicate->generalized #{}))
        freshened (freshen-expressions rule query)]
    (is (= 'Houston
           (-> freshened prot/consequent prot/terms first))
        "freshen-expressions is not propagating absolute constants.")
    (is (= '[Houston ?some-var]
           (prot/terms query)
           (prot/terms (prot/consequent freshened))))
    (is (and
         (not= '[Houston ?x]
               (prot/terms (first (prot/antecedents freshened))))
         (not= '[?b ?x] (second (prot/antecedents freshened))))
        "Some of the 'freshened' expressions are the same as before.")))
