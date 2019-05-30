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

(ns eva.query.datalog.rule
  (:require [eva.query.datalog.protocols :as p]
            [eva.query.util :as qutil :refer [simple-unify]]
            [clojure.core.unify :as u]
            [eva.error :refer [insist]])
  (:import [java.util HashMap]))

(defn fresh [form]
  (let [renamed (HashMap.)]
    (letfn [(rename-lvar [x]
              (.computeIfAbsent renamed x (reify java.util.function.Function
                                            (apply [_ x] (qutil/fresh-lvar x)))))]
      (clojure.walk/postwalk
       #(if (u/lvar? %) (rename-lvar %) %)
       form))))

(defrecord Rule
    [antecedents consequent]
  p/Expressive
  (expression [_]
    (apply list (p/expression consequent)
           :<=
           (map p/expression antecedents)))
  p/Rule
  (antecedents [_] antecedents)
  (consequent [_] consequent)
  (freshen [this]
    (let [[antecedents consequent] (fresh [antecedents consequent])]
      (->Rule antecedents consequent)))
  (subst [_ unifier]
    (->Rule (u/subst antecedents unifier)
            (u/subst consequent unifier))))

(defn rule
  ([consequent antecedents]
   (insist (and (satisfies? p/Predicate consequent)
                (every? (partial satisfies? p/Predicate) antecedents))
           "The consequent and antecedents of a rule must be predicates.")
   (insist (p/rule? consequent)
           "The consequent of a rule must be tagged as a rule predicate.")
   (insist (not (p/negated? consequent))
           "The consequent of a rule cannot be negated.")
   (insist (some (complement p/negated?) antecedents)
           "It is not allowed for all antecedents of a rule to be negated.")
   (let [idxs (set (p/required-bindings consequent))
         optional-terms (keep-indexed #(when-not (idxs %) %2) (-> consequent p/terms))]
     ;; So, technically, it's safe if the term is guaranteed to be bound by the time
     ;; the rule runs. I'm enforcing only that, because it is (a) correct and (b) makes
     ;; it far, far easier to compile into datalog with ?src-var lvars. But this doesn't
     ;; require knowledge dependency of implementation details of the query.dialect code.
     (let [ant-lvars (into #{} (comp (mapcat p/terms) (filter u/lvar?)) antecedents)]
       (doseq [term (filter u/lvar? optional-terms)]
         (insist (contains? ant-lvars term)
                 (format "A rule for %s has the term %s in its head, but not in its body."
                         (p/sym consequent) term)))))
   (->Rule antecedents consequent)))

(defn freshen-expressions
  "Freshens lvars in the rule, and propagates absolute constants; i.e., constants
  that are constant across the entire search space."
  [rule query]
  (let [rule (p/freshen rule)
        mgu (simple-unify (-> rule p/consequent p/terms) {} (p/terms query))]
    (p/subst rule mgu)))
