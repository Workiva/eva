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

(ns eva.query.datalog.qsqr.protocols)

(defprotocol State
  (rule-log [state] [state queries] "Returns/sets a nested datastructure representing the generalized rule queries that have been seen already: { predicate-symbol { decoration generalized-pred }} The decoration structure is exactly what is returned by eva.query.datalog.protocols/decoration; the generalized-pred is an eva.query.datalog.predicate.GeneralizedPredicate.")
  (extension-log [state] [state queries] "Returns/sets a nested datastructure representing the generalized edb queries that have been seen already: { predicate-symbol { decoration generalized-pred }} The decoration structure is exactly what is returned by eva.query.datalog.protocols/decoration; the generalized-pred is an eva.query.datalog.predicate.GeneralizedPredicate.")
  (evaluation-log [state] [state queries] "Returns/sets a nested datastructure representing the generalized evaluable queries that have been seen already: { predicate-symbol { decoration generalized-pred }} The decoration structure is exactly what is returned by eva.query.datalog.protocols/decoration; the generalized-pred is an eva.query.datalog.predicate.GeneralizedPredicate.")
  (add-query [state query] "Adds this generalized predicate to the state blob; if a matching predicate/decoration combination already exists, this merges the two.")
  (derived [state] "Returns a nested datastructure representing the current derivations of constants for particular predicates: { relation-symbol #{ constant-tuples }}")
  (reset-bindings [state])
  (bindings [state] [state x] "Returns/sets a set of unifier maps: #{ unifier, unifier, unifier ... }. (unifier = { ?a ?b, ?b c ...} )")
  (select-antecedent [state predicates] "From the supplied sequence of predicates, selects one to evaluate next. Returns [selected others]. May throw an exception if, for any reason, a predicate cannot be selected.")
  (select-rule [state query rules] "From the supplied sequence of rules, selects one to employ next. Returns [selected others]. May throw an exception if, for any reason, a rule cannot be selected."))
