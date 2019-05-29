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

(ns eva.query.datalog.predicate
  (:require [eva.query.datalog.protocols :as p]
            [eva.query.util :as qutil]
            [clojure.core.unify :as u]
            [eva.error :refer [insist]]))

(defrecord Predicate
    [symbol terms -? type required]
  p/Expressive
  (expression [_] (apply list symbol terms))
  p/Predicate
  (required-bindings [_] required)
  (sym [_] symbol)
  (terms [_] terms)
  (terms [this v] (assoc this :terms v))
  (terms [this f v] (update this :terms f v))
  (evaluable? [_] (= type :evaluable))
  (extensional? [_] (= type :extensional))
  (rule? [_] (= type :rule))
  (negated? [_] -?))

(defrecord GeneralizedPredicate
    [predicate decoration bound-terms bindings]
  p/Expressive
  (expression [_] (p/expression predicate))
  p/Predicate
  (required-bindings [_] (p/required-bindings predicate))
  (sym [_] (p/sym predicate))
  (terms [_] (p/terms predicate))
  (terms [_ v] (p/terms predicate v))
  (terms [_ f v] (p/terms predicate f v))
  (evaluable? [_] (p/evaluable? predicate))
  (extensional? [_] (p/extensional? predicate))
  (rule? [_] (p/rule? predicate))
  (negated? [_] (p/negated? predicate))
  p/Decorated
  (decoration [_] decoration)
  (bound-terms [_] bound-terms)
  (bound-terms [this v] (assoc this :bound-terms v))
  (bound-terms [this f v] (update this :bound-terms f v)))

(defn predicate
  "Returns a function that takes a predicate's terms and modifiers and
  generates a Predicate object."
  [sym {:keys [term-count required type]
        :or {required []}}]
  (insist (and (sequential? required)
               (every? number? required))
          "The correct format for the (optional) required-bindings argument is a sequential collection of integer indices.")
  (insist (or (empty? required)
              (= term-count :variable) ;; TODO:
              (> term-count (apply max required))) "Index out of range in required bindings.")
  (insist (symbol? sym) "The predicate head must be a symbol.")
  (insist (some #(= type %) [:extensional :rule :evaluable])
          "A predicate must be tagged with exactly one of :extensional, :rule, and :evaluable. ")
  (fn [terms & modifiers]
    (insist (coll? terms) "You must supply a collection of terms.")
    (insist (or (and (or (empty? required) ;; TODO:
                      (> (count terms) (apply max required))) ;; TODO:
                 (= term-count :variable)) ;; TODO:
                (= term-count (count terms)))
            (str "Wrong number of terms passed to " sym ". Expected " term-count ", received " (count terms)))
    (let [modifiers (into #{} modifiers)]
      (insist (not (and (or (modifiers :+) (modifiers :positive))
                        (or (modifiers :-) (modifiers :negative))))
              "A predicate cannot be tagged both :+ / :positive and :- / :negative")
      (->Predicate sym
                   (vec terms)
                   (if (or (modifiers :-) (modifiers :negative)) true false)
                   type
                   (sort (distinct required))))))

(defn extensional-predicate
  [sym {:keys [type] :as options-map}]
  (insist (or (nil? type) (= :extensional type))
          "An edb-predicate is, by definition, of :extensional type.")
  (predicate sym (assoc options-map :type :extensional)))

(defn rule-predicate
  [sym {:keys [type] :as options-map}]
  (insist (or (nil? type) (= :rule type))
          "A rule-predicate is, by definition, of :rule type.")
  (predicate sym (assoc options-map :type :rule)))

(defn evaluable-predicate
  [sym {:keys [type] :as options-map}]
  (insist (or (nil? type) (= :evaluable type))
          "An evaluable-predicate is, by definition, of :evaluable type.")
  (predicate sym (assoc options-map :type :evaluable)))

(defn- step:constant->lvar
  [[rewritten unifier reverse-unifier] idx term]
  (if (u/lvar? term)
    [(conj rewritten term) unifier reverse-unifier]
    (let [new-lvar (gensym (str "?auto" qutil/genned-lvar-separator))]
      [(conj rewritten new-lvar)
       (assoc unifier new-lvar term)
       (assoc reverse-unifier idx new-lvar)])))

(defn constants->lvars
  "Takes a predicate some of whose terms may be constants,
  and a set of bindings.
  Returns [new-pred new-bindings], where all terms in new-pred are lvars
  and new-bindings is the corresponding unification-map merged into every
  bindings map in bindings."
  [predicate bindings]
  (if (every? u/lvar? (p/terms predicate))
    [predicate bindings]
    (let [[rewritten
           unifier
           reverse-unifier] (reduce-kv step:constant->lvar
                                       [[] {}]
                                       (vec (p/terms predicate)))]
      [(p/terms predicate #(reduce (partial apply assoc) % %2) reverse-unifier)
       (->> bindings (map (partial merge unifier)) set)])))
