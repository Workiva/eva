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

(ns eva.query.datalog.program
  (:require [eva.query.datalog.protocols :as p]
            [clojure.core.unify :as u]
            [eva.error :refer [insist]]))

(defn- subst-in-gen
  "We have three datastructures:
  decoration: [idx-1 idx-2 ... idx-n]
  terms: [term-1 term-2 ... term-k]
  bindings: [bind-1 bind-2 ... bind-n]

  We want to take the values in the bindings vector and insert them into the terms
  in particular positions. Those positions are designated by the values in the
  decoration vector: bind-*-1 goes into position idx-1 in terms, bind-*-2 goes
  into position idx-2 in terms. Etc.

  This function takes the decoration and generates a function that takes a terms
  vector and a bindings vector. The names are for QSQR reasons. The function generation
  is for performance and GC reasons."
  [decoration]
  (let [idx->bindings-idx (zipmap decoration (range))]
    (fn [terms bindings]
      (reduce-kv (fn [res idx term] ;; <== idx is only an idx if terms is a vector
                   (conj res
                         (if-let [bindings-idx (idx->bindings-idx idx)]
                           (nth bindings bindings-idx)
                           term)))
                 [] terms))))

(defn subst-in
  [decoration terms coll-of-bindings]
  (let [subster (subst-in-gen decoration)]
    (map (partial subster terms) coll-of-bindings)))

(defrecord Program
    [edbs evaluators rule-catalog]
  p/Program
  (extension [program gen-pred]
    (let [terms (p/terms gen-pred)
          terms (sequence (comp (map #(replace % terms)) ;; TODO: change data structure representation of bindings in gen-pred?
                                (distinct))
                          (:bindings gen-pred))]
      (p/extensions (get edbs (p/sym gen-pred)) terms)))
  (evaluation [program gen-pred]
    (let [terms (subst-in (p/decoration gen-pred) (p/terms gen-pred) (p/bound-terms gen-pred))]
      (p/evaluations (get evaluators (p/sym gen-pred)) terms)))
  (relevant-rules [program pred]
    (get rule-catalog (p/sym pred)))
  (range-restricted? [program rule]
    (every? true?
            (for [term (-> rule p/consequent p/terms)
                  :when (u/lvar? term)
                  ant (p/antecedents rule)
                  :when (some #(= term %) (p/terms ant))]
              (cond (p/rule? ant)
                    (every? (partial p/range-restricted? program)
                            (p/relevant-rules program ant))
                    (p/extensional? ant)
                    true
                    (p/evaluable? ant)
                    (-> (some #(= term %)
                              (map #(nth (p/terms ant) %) (p/required-bindings ant)))
                        boolean))))))

(defn program
  [& {:keys [edbs evaluators rules]}]
  (insist (or (nil? edbs) (map? edbs)) "edbs, if supplied, must be a map symbol->EDB")
  (when (some? edbs)
    (insist (and (every? (partial satisfies? p/EDB) (vals edbs))
                 (every? symbol? (keys edbs)))
            "edbs, if supplied, must be a map symbol->EDB"))
  (insist (or (nil? evaluators) (map? evaluators)) "evaluators, if supplied, must be a map symbol->Evaluable")
  (when (some? evaluators)
    (insist (and (every? (partial satisfies? p/Evaluable) (vals evaluators))
                 (every? symbol? (keys evaluators)))
            "evaluators, if supplied, must be a map symbol->Evaluable"))
  (insist (or (nil? rules)
              (and (coll? rules) (empty? rules))
              (every? (partial satisfies? p/Rule) rules))
          "rules, if supplied, must contain only objects satisfying the protocol Rule")
  (let [update-fn (fn [catalog rule] (update catalog
                                             (-> rule p/consequent p/sym)
                                             (fnil conj [])
                                             rule))
        rule-catalog (reduce update-fn {} rules)]
    (doseq [rule rules
            pred (conj (p/antecedents rule) (p/consequent rule))]
      (cond (p/evaluable? pred)
            (insist (contains? evaluators (p/sym pred))
                    "An evaluable predicate occurs in the program that is not documented in the evaluator map.")

            (p/extensional? pred)
            (insist (contains? edbs (p/sym pred))
                    "An extensional predicate occurs in the program that is not documented in the edbs map.")

            (p/rule? pred)
            (insist (contains? rule-catalog (p/sym pred))
                    "An rule predicate occurs in the program for which there is no matching rule in the catalog.")))
    (->Program edbs evaluators rule-catalog)))

#_(defn strongly-safe?
    [rule]
    (and (range-restricted? rule)
         (every? (into #{} (mapcat variables (filter base? (predicates rule))))
                 (mapcat variables (filter evaluable? (predicates rule))))))

#_(defn secure?
    ([rules variable]
     (secure? rules #{} variable))
    ([rules unsecured variable]
     (or (some #(contains? % variable)
               (filter (complement evaluable?) rules))
         (some (fn [rule]
                 (let [deps (safety-dependencies rule)]
                   (and (contains? (val deps) variable)
                        (every? (every-pred
                                 (complement unsecured) ;; <- a variable can't secure itself
                                 (partial secure? rules (conj unsecured variable)))
                                (key deps)))))
               rules))))
