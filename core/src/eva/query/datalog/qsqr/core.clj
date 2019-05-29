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

(ns eva.query.datalog.qsqr.core
  (:require [eva.query.datalog.protocols :as p]
            [eva.query.datalog.rule :as rule]
            [eva.query.datalog.qsqr.protocols :as qp]
            [eva.query.datalog.qsqr.state :as state]
            [eva.query.datalog.predicate :as pred]
            [eva.query.util :as qutil]
            [clojure.core.unify :as u]
            [utiliva.alpha :refer [sreduce]]
            [eva.query.trace :as special]))

(declare query*)
(defn antecedent-step
  "Processes one particular antecedent of a rule, adding or removing from the current
  set of viable bindings. gen-predicate, as the name suggests, must be a generalized
  predicate. Returns the new state blob."
  [program state gen-predicate]
  (special/trace "Antecedent step: " (p/expression gen-predicate))
  (let [novel-query (state/novel-generalization state gen-predicate)
        state (if-not novel-query
                (do (special/trace "  > but nothing was novel about the generalized predicate.")
                  state)
                (let [state (if (and (p/negated? novel-query) (p/rule? novel-query))
                              state
                              (qp/add-query state novel-query))]
                  (cond (p/extensional? gen-predicate)
                        (do (special/trace " > It's extensional, and its result is:")
                            (state/update-derived state gen-predicate
                                                  (special/trace-spy "(take 10): " (partial take 10) (p/extension program novel-query))))

                        (p/evaluable? gen-predicate)
                        (state/update-derived state gen-predicate
                                              (special/trace-spy "(take 10): " (partial take 10) (p/evaluation program novel-query)))

                        (p/rule? gen-predicate)
                        (let [subquery-state (state/trim-bindings state novel-query)] ;; substantial efficiency boost
                          (special/trace " > It's a rule, turning into a SUBQUERY:")
                          (let [r (-> (query* program subquery-state novel-query)
                                      (qp/bindings (qp/bindings state)))]
                            (special/trace " < RETURNED from the subquery.")
                            r)))))]
    (let [r (state/conjunct-bindings state gen-predicate)]  ;; lvars -> constants, negation removes matches
      (special/trace " - Bindings: " (qp/bindings r))
      r)))

(defn rule-step
  "Processes one particular rule whose head matches the current predicate, query.
  query should be a generalized predicate."
  [program query state rule]
  (let [unified-rule (rule/freshen-expressions rule query)]
    (special/trace "Rule step: " (p/expression unified-rule))
    (sreduce qp/select-antecedent
             (fn [state ant]
               (let [[ant bindings] (pred/constants->lvars ant (qp/bindings state))
                     state (qp/bindings state bindings)
                     ant (state/predicate->generalized ant bindings)
                     state (antecedent-step program state ant)]
                 (if (empty? (qp/bindings state))
                   (reduced state)
                   state)))
             state
             (p/antecedents unified-rule))))

(defn query-step
  "Grabs all rules relevant to the current query, and iterates over them, returning
  a new state containing the union of the bindings given by processing each rule."
  [program state query]
  (special/trace "Query step: " (p/expression query))
  (let [bindings (qp/bindings state)]
    (let [relevant-rules (p/relevant-rules program query)]
      (assert (seq relevant-rules)
              (format "The program has no relevant rules for the expression: %s"
                      (p/expression query)))
      (loop [disjunctive-bindings #{}
             state state
             [rule rules] (qp/select-rule state query relevant-rules)]
        (let [state (as-> (rule-step program query state rule) state
                      (state/update-derived state query (state/extract-derived state query)))
              reset-state (qp/bindings state bindings)
              disjunctive-bindings (into disjunctive-bindings (qp/bindings state))]
          (if (empty? rules)
            (do (special/trace " > Disjuncive bindings:\n" disjunctive-bindings "\n  ^^^^^^^^^^^")
                (qp/bindings state disjunctive-bindings))
            (recur disjunctive-bindings
                   reset-state
                   (qp/select-rule reset-state query rules))))))))

(defn query*
  "Top of the QSQR algorithm. Takes a snapshot of current state, runs query-step
  on the current predicate, and returns if no new relations have been derived.
  q must be a generalized predicate."
  [program state q]
  (special/trace "Current goal: " q)
  (let [orig-bindings (qp/bindings state)
        orig-rule-log (qp/rule-log state)]
    (loop [prev-derived (qp/derived state)
           state (query-step program state q)]
      (if (= prev-derived (qp/derived state))
        state
        (let [reset-state (-> state (qp/bindings orig-bindings) (qp/rule-log orig-rule-log))]
          (recur (qp/derived state) (query-step program reset-state q)))))))

(defn query
  "Entry to the QSQR algorithm. Defaults to no initial bindings, but a set may be passed
  in: #{ {'?lvar-1 some-value, ...} ...}. Returns, in vector form, derived relations
  for the query, respecting init-bindings constraints."
  ([program q]
   (query program q #{{}}))
  ([program q init-bindings]
   (let [exemplar (-> (p/relevant-rules program q)
                      first p/consequent)
          ;; ^^ an example of this predicate expression pulled from the rules; preserves ?src__ vs. ?auto__.
         terms (p/terms exemplar)
         binding (qutil/simple-unify terms (map #(if (u/lvar? %) '_ %) (p/terms q)))
         ;; ^^ created by unifying the constants in the original query with the exemplar terms
         bindings (into #{} (map (partial merge binding)) init-bindings)
         q-new (state/predicate->generalized exemplar bindings)
         init-state (-> state/init-state (qp/add-query q-new) (qp/bindings bindings))
         result (query* program init-state q-new)
         all-derived (get (:derived result) (p/sym q))]
     (set (filter (partial qutil/unifies? (p/terms q)) all-derived)))))
