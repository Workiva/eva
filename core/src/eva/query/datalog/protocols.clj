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

(ns eva.query.datalog.protocols)

(defprotocol EDB
  (extensions [edb coll-of-terms] "Given an extensional predicate and a collection of groups of terms, returns a sequence containing all distinct matches for any of the term groups."))

(defprotocol Evaluable
  (evaluations [ev coll-of-terms] "Given an evaluable predicate and a collection of groups of terms, returns a sequence containing all distinct matches for any of the term groups."))

;; Nota Bene:
;; EDB and Evaluable both capture the spirit of a protocol I would just call Relational.
;; Extensional relations and Evaluable relations are, for most practical purposes,
;; functionally identical. Early discussion resulted in the decision to treat them
;; distinctly for the sake of potential future unknown modifications/optimizations.
;;
;; In terms of intent, EDB sources/predicates are assumed to be finite and thus
;; range-restricted. Evaluable sources/predicates could be anything -- once you
;; allow arbitrary Evaluable predicate into the program, safety guarantees go out
;; the window.

(defprotocol Program
  (extension [program pred] "Returns a collection of tuples representing fully-bound terms of the extensional predicate.")
  (evaluation [program pred] "Returns a collection of tuples representing fully-bound terms of the evaluable predicate.")
  (relevant-rules [program pred] "Returns a collection of rules whose consequent consists of the supplied predicate.")
  (range-restricted? [program rule] "In this program, is this rule range-restricted?"))

(defprotocol Rule
  (antecedents [rule] "Returns a collection containing the antecedent predicates of this rule.")
  (consequent [rule] "Returns the predicate that is the consequent of this rule.")
  (freshen [rule] "Freshens lvars in both the consequent and the antecedents.")
  (subst [rule unifier] "Applies the unifier to both consequent and antecedents."))

(defprotocol Predicate ;; Nota bene: this protocol presumes that predicates are the result of an up-front "compilation" step of the program.
  (required-bindings [rule] "Returns a vector of indices, corresponding to variables that must be bound at rule invocation.")
  (sym [pred] "Returns the symbol representing this predicate.")
  (terms [pred] [pred v] [pred f v] "Returns/sets/updates an ordered collection of the terms.")

  (evaluable? [pred] "Returns true if this predicate is evaluable -- answers by calculation.")
  (extensional? [pred] "Returns true if this predicate is backed by an edb.")
  (rule? [pred] "Returns true if this predicate represents the head of a rule in the program.")
  (negated? [pred] "Returns true if this *instance* of the predicate is negated."))

(defprotocol Expressive
  (expression [pred] "Returns `(~(sym pred) ~@(terms pred)).")) ;; TODO:

(defprotocol Decorated
  (decoration [pred] "Returns a vector containing the indexes which are bound")
  (bound-terms [pred] [pred v] [pred f v] "Returns/sets/updates a set of bindings corresponding to the decoration."))
