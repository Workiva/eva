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

(ns eva.query.dialect.translation.compile.pass2
  (:require [eva.query.datalog.predicate :as pred]
            [eva.query.datalog.rule :as rule]
            [eva.query.datalog.program :as prog]
            [eva.query.datalog.qsqr.core :as qsqr]
            [eva.query.dialect.spec :as qs]
            [eva.query.dialect.sandbox :as sandbox]
            [eva.query.dialect.pull.core :refer [pull pull-many]]
            [eva.query.dialect.translation.edb :refer [edb-composer-gen-spec]]
            [eva.query.dialect.translation.error :refer :all]
            [eva.error :refer [insist raise]]
            [ichnaie.core :refer [tracing]]
            [clojure.spec.alpha :as s]
            [clojure.pprint :refer [pprint]]
            [utiliva.core :refer [zip-to map-keys]])
  (:refer-clojure :exclude [compile]))

;;;;; helpers! ;;;;

(defn query-rule-name [query] (get-in query [:query-rule :rule-head :rule-name]))
(defn query-rule-src-vars [query] (get-in query [:query-rule :rule-head :src-vars]))
(defn query-rule-vars [query] (-> query :query-rule qs/all-rule-vars))

(defmulti extract-lvar (fn [[type _]] type))

(defmethod extract-lvar :pull-expr [[_ pull-expr]] (:variable pull-expr))
(defmethod extract-lvar :variable [[_ var]] var)
(defmethod extract-lvar :unbound [_] nil)
(defmethod extract-lvar :aggregate [[_ agg]]
  ;; TODO: matches current behavior, does *not* satisfy the general contract for aggregate functions
  (extract-lvar (last (:fn-args agg))))

(defmulti ->process-fn (fn [[type _]] type))

(defmethod ->process-fn :variable [_] first)
(defmethod ->process-fn :pull-expr [_] first)
(defmethod ->process-fn :aggregate [[_ agg]]
  (let [fn-name (:aggregate-fn-name agg)]
    (if-let [fn (ns-resolve sandbox/*sandbox-ns* fn-name)]
      (apply partial fn (->> agg :fn-args butlast (map second)))
      (raise-translation-error :unresolved-sym
                               (format "unknown aggregate fn: '%s'." fn-name)
                               {:fn-sym fn-name}))))

(defn datom-filler
  "Fills in implicit vars."
  [terms]
  (take 6 (concat terms (repeat '_))))

(defn vlens [idx] (fn [v] (nth v idx)))

(defn lazy-tuples
  [find-terms result-group]
  (for [term find-terms]
    ((:process term)
     (sequence (-> term :idx vlens map) result-group))))

(defmulti ->find-terms-maps (fn [[find-spec-type find-spec-detail :as find-spec] query] find-spec-type))

(defmulti realize (fn [[find-spec-type :as find-spec]] find-spec-type))

(defn pull? [[type]] (= type :pull-expr))

(defn extract-pull-pattern
  [[_ pull-expr :as input]]
  (let [[pattern-type pattern] (:pattern pull-expr)
        extracted-pattern
        (case pattern-type
          :pattern-data-literal (s/unform ::qs/pattern-data-literal pattern)
          ;; See: eva.query.dialect.sandbox
          :input-name (get @(resolve 'pattern-vars) pattern))]
    extracted-pattern))

(defn transposev [coll-of-colls] (apply mapv vector coll-of-colls))

(defn- resolve-pulls-gen
  "Generates a composition of pull functions that will resolve all pulls in the result.
  =multi?= set to true generates pull-many fns."
  [f-c-a multi?]
  (let [pull-fn (if multi? pull-many pull)
        elements f-c-a
        pull-fns (for [idx (-> elements count range)
                       :let [element (nth elements idx)
                             db-sym (get (second element) :src-var '$)]
                       :when (pull? element)]
                   (fn [syms->dbs tuplev]
                     (update tuplev
                             idx
                             #(pull-fn (get syms->dbs db-sym) (-> element extract-pull-pattern) %))))]
    (fn [syms->db results] ;; TODO: inelegant
      (if (empty? results)
        ()
        (let [results (if multi? (transposev results) results)
              results (reduce (fn [results pull-fn] (pull-fn syms->db results))
                              results
                              pull-fns)
              results (if multi? (transposev results) results)]
          results)))))

(defn pull? [[type]] (= :pull-expr type))

(defmethod realize :find-rel [[_ find-spec]]

  (let [resolve-pulls (resolve-pulls-gen find-spec true)]
    (cond (some pull? find-spec)
          (resolve-pulls-gen find-spec true)
          :else
          (fn [_ results] (mapv vec results)))))

(defmethod realize :find-tuple [[_ find-spec]]
  (cond (some pull? (:find-elems find-spec))
        (let [resolve-pulls (resolve-pulls-gen (:find-elems find-spec) false)]
          (fn [syms->dbs results]
            (resolve-pulls syms->dbs (-> results first vec))))
        :else
        (fn [_ results] (-> results first vec))))

(defmethod realize :find-coll [[_ find-spec]]
  (cond (pull? (:find-elem find-spec))
        (let [expr (-> find-spec :find-elem)
              db-sym (get (second expr) :src-var '$)]
          (fn [syms->dbs results]
            (->> results
                 (map first)
                 (pull-many (get syms->dbs db-sym) (extract-pull-pattern expr))
                 vec)))
        :else (fn [_ results] (mapv first results))))

(defmethod realize :find-scalar [[_ find-spec]]
  (cond (pull? (:find-elem find-spec))
        (let [pull-pattern (-> find-spec :find-elem)
              db-sym (get (second pull-pattern) :src-var '$)]
          (fn [syms->dbs results]
            (->> results
                 ffirst
                 (pull (get syms->dbs db-sym) (extract-pull-pattern pull-pattern)))))
        :else (fn [_ results] (ffirst results))))

(defn predicate-expression->datalog
  "Given the map produced by predinfo->pred-gens and a particular predicate expression,
  this produces a Predicate object for use by the datalog engine."
  [sym->pred-gens pred-expr]
  (let [[sym & [sign & terms]] pred-expr
        pred-gen (get sym->pred-gens sym)]
    (when-not (some? pred-gen)
      (raise-translation-error :unknown-predicate
                               "predicate does not exist."
                               {:symbol sym}))
    (pred-gen terms sign)))

(defn rule-expressions->datalog
  "Given the simplified rule-expressions (as returned by process-ast) and the map
  from predicate name/symbol to query.datalog. Predicate constructors (as returned by
  process-ast), this returns a collection of rules compatible with eva.query.datalog."
  [rule-expressions sym->pred-gens]
  (for [[consequent antecedents] rule-expressions]
    (rule/rule (predicate-expression->datalog sym->pred-gens consequent)
               (map (partial predicate-expression->datalog sym->pred-gens) antecedents))))

(defn projector-gen
  "Query results are grouped by the variables that do not occur in
  aggregate expressions. This produces a function that pulls out exactly
  those variables from each raw result of the Datalog query."
  [find-terms]
  (let [proj-fns (for [term find-terms
                       :when (:grouped? term)]
                   (vlens (:idx term)))]
    (if (empty? proj-fns)
      (constantly nil) ;; If not grouping at all, still need to project to *something*.
      (apply juxt proj-fns))))

(defn lazy-results-gen
  "Shapes the raw output of the Datalog query as appropriate for the find clause.
  This is lazy upon lazy, because in the case of scalar or tuple outputs, the
  full processing of query results would be a waste of time."
  [find-terms]
  (let [projector (projector-gen find-terms)]
    (fn [results]
      (map (partial lazy-tuples find-terms)
           (->> results (group-by projector) vals)))))

(defn ->find-terms-map
  [init-query elem]
  ;; variable / aggregate / pull
  (let [lvar (extract-lvar elem)
        idx (.indexOf ^clojure.lang.PersistentVector (vec (:terms init-query)) lvar)
        process-fn (->process-fn elem) ;; pulls happen later.
        grouped? (or (= :variable (first elem))
                     (pull? elem))]
    {:process process-fn
     :idx idx
     :grouped? grouped?}))

(defmethod ->find-terms-maps :find-rel [[_ find-spec] init-query]
  (map (partial ->find-terms-map init-query) find-spec))

(defmethod ->find-terms-maps :find-tuple [[_ find-spec] init-query]
  (map (partial ->find-terms-map init-query) (:find-elems find-spec)))

(defmethod ->find-terms-maps :find-coll [[_ find-spec] init-query]
  [(->find-terms-map init-query (:find-elem find-spec))])

(defmethod ->find-terms-maps :find-scalar [[_ find-spec] init-query]
  [(->find-terms-map init-query (:find-elem find-spec))])

(defn shape-results-gen
  "Produces a function that takes a table of named databases and the raw results of the datalog query,
  and turns them into the shape specified by the query. syms->db table is required
  so that pull expressions can be resolved."
  [init-query find-spec]
  (let [find-terms (->find-terms-maps find-spec init-query)
        lazy-results-f (lazy-results-gen find-terms)
        realize-f (realize find-spec)]
    (fn [syms->dbs raw-results]
      (realize-f syms->dbs (lazy-results-f raw-results)))))

(defn output-gen
  "Given the :find and :with clauses of a query, this returns a function that will
  take the result of the QSQR query and shape it appropriately for return to the user."
  [init-query find-spec]
  (shape-results-gen init-query find-spec))

(defmulti bind?->fn (fn [[type]] type))

;; No-op input bindings.
(defmethod bind?->fn :src-var [_] nil)
(defmethod bind?->fn :rules-var [_] nil)
(defmethod bind?->fn :pattern-var [_] nil)

(defmethod bind?->fn :binding [[_ binding]] (bind?->fn binding))
(defmethod bind?->fn :bind-scalar [[_ scalar]]
  (let [sym scalar]
    (fn [bindings scalar]
      (map #(assoc % sym scalar) bindings))))
(defmethod bind?->fn :bind-coll [[_ coll]]
  (let [sym (:variable coll)]
    (fn [bindings coll]
      (for [binding bindings
            scalar (sequence (distinct) coll)] ;; de-dupe on our side just in case.
        (assoc binding sym scalar)))))
(defmethod bind?->fn :bind-tuple [[_ tuple]]
  (let [syms (map extract-lvar tuple)]
    (fn [bindings tuple]
      (for [binding bindings]
        (merge binding (zipmap syms tuple))))))
(defmethod bind?->fn :bind-rel [[_ rel]]
  (let [syms (map extract-lvar (first rel))]
    (fn [bindings rel]
      (for [binding bindings
            tuple (sequence (distinct) rel)] ;; de-dupe on our side just in case.
        (merge binding (zipmap syms tuple))))))

(defn input-binder-gen
  "Given the :in clause of a query, this returns a function that will take the
  corresponding runtime inputs and generate an initial set of bindings for the QSQR
  algorithm."
  [inputs-list]
  (let [idx->fn (into {}
                      (keep-indexed (fn [idx input] (when-let [f (bind?->fn input)] [idx f])))
                      inputs-list)]
    (fn [inputs]
      (insist (= (count inputs-list)
                 (count inputs))
              (format "Query expected %s inputs but received %s."
                      (count inputs-list) (count inputs)))
      (reduce-kv #(if-let [f (idx->fn %2)]
                    (f % %3)
                    %)
                 '({})
                 (vec inputs)))))

(defn pattern-var? [[[type]]]
  (= type :pattern-var))

(defn pattern-var->sym [[_ sym]]
  sym)

(defrecord CompiledQuery
    [src-var datom-pred inputs->edb inputs->bindings query-ast query-rule-ast rules top-query result->outputs function inspect]
  clojure.lang.IFn
  (invoke [this inputs]
    (when (nil? function)
      (throw (IllegalStateException. "This query has not been fully compiled -- it requires a rules input.")))
    (function inputs))
  (applyTo [this ^clojure.lang.ISeq input] (function (first input))))


(defn compile-processed-form
  [{:keys [query
           datom-pred
           rule-expressions
           sym->pred-gens
           sym->evals]
    :as processed-query-form}]

  (let [sym->pred-gens (assoc sym->pred-gens datom-pred
                              (let [f (pred/extensional-predicate datom-pred
                                                                  {:required [],
                                                                   :term-count :variable,
                                                                   ;; ^^^ because variable, datom-filler probably unnecessary.
                                                                   :type :extensional})]
                                (fn [terms & modifiers]
                                  (apply f (datom-filler terms) modifiers))))

        init-query ((sym->pred-gens (query-rule-name query))
                    (concat (query-rule-src-vars query)
                            (query-rule-vars query)))
        datalog-rules (rule-expressions->datalog rule-expressions sym->pred-gens)

        result->outputs (output-gen init-query (qs/find-spec query))

        sandbox-ns (sandbox/create-eval-ns)
        inputs-list (qs/inputs query)

        inputs->edb (edb-composer-gen-spec inputs-list)

        inputs->bindings (input-binder-gen inputs-list)

        ;; This is the function that actually executes the query:
        function (fn [inputs]
                   (let [{:keys [sym->edb datalog-edb]} (inputs->edb inputs)
                         pattern-vars (into {}
                                            (comp (zip-to inputs)
                                                  (filter pattern-var?)
                                                  (map-keys pattern-var->sym))
                                            (-> query qs/inputs))]
                     (with-bindings {#'sandbox/*sandbox-ns* sandbox-ns
                                     #'*ns* sandbox-ns
                                     (ns-resolve sandbox-ns 'sym->edb) sym->edb
                                     (ns-resolve sandbox-ns 'pattern-vars) pattern-vars}
                       (let [program (prog/program :edbs {datom-pred datalog-edb},
                                                   :evaluators sym->evals,
                                                   :rules datalog-rules)
                             init-bindings (inputs->bindings inputs)
                             query-results (tracing "eva.q: QSQR"
                                                    (qsqr/query program init-query init-bindings))]
                         (tracing "eva.q: shaping query results"
                                  (result->outputs sym->edb query-results))))))

        ;; This is a function that lets us inspect the compiled datalog, given the inputs:
        inspect (fn [inputs]
                  (let [{:keys [sym->edb datalog-edb]} (inputs->edb inputs)]
                    {:program (prog/program :edbs {datom-pred datalog-edb},
                                            :evaluators sym->evals,
                                            :rules datalog-rules)
                     :sym->edb sym->edb
                     :init-bindings (inputs->bindings inputs)}))]
    (doseq [[head antecedents] rule-expressions]
      (insist (and (every? some? head)
                   (every? true? (for [ant antecedents]
                                   (every? some? ant))))
              (format "The rule expression %s contains nil. Its antecedents are: %s" (pr-str head) (pr-str antecedents))))
    (map->CompiledQuery {:datom-pred datom-pred
                         :inputs->edb inputs->edb
                         :inputs->bindings inputs->bindings
                         ;;:query-rule-ast query-rule-ast
                         :input-rules datalog-rules
                         :top-query init-query
                         :result->outputs result->outputs
                         :function function
                         :inspect inspect})))
