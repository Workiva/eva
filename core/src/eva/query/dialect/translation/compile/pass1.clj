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

(ns eva.query.dialect.translation.compile.pass1
  (:require [eva.query.dialect.sandbox :as sandbox]
            [eva.query.datalog.evaluable :as eval]
            [eva.query.dialect.functions :as fn]
            [eva.query.dialect.spec :as qs]
            [eva.query.datalog.predicate :as pred]
            [eva.query.dialect.translation.error :refer :all]
            [clojure.spec.alpha :as s]
            [clojure.pprint :refer [pprint]])
  (:refer-clojure :exclude [compile]))



;;;;;;;;;;;;;;;;;;;;;;;;;
;; weird helper things
;;;;;;;;;;;;

(defn src-lvar "Gensyms a new ?src__ lvar" [] (gensym "?src__"))

(defn $->?src*
  "Takes a sequence of $* symbols and returns a map from them to ?src__ symbols."
  [$s]
  (into {}
        (keep #(when (s/valid? ::qs/src-var %)
                 [% (src-lvar)]))
        $s))

(defn lvar-is-src-var? "Is it a ?src__ lvar?" [lvar] (clojure.string/starts-with? (name lvar) "?src__"))

;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn ->where-rule-vars
  "Produce a datastructure of spec-conformant rule-vars given the find-lvars and
   input-lvars of a where clause."
  [find-lvars input-lvars]
  (let [in-set (set input-lvars)
        grpd (group-by #(contains? in-set %) (concat find-lvars input-lvars))
        rule-vars (qs/->rule-vars (get grpd false) (get grpd true))]
    rule-vars))

(defn ->where-rule
  "Given a :find clause and :where clause, this generates a new rule by
  embedding all the expressions as the antecedents of a new rule, with a new rule
  consequent defined containing the variables designated of interest by the :find
  clause."
  [query]
  (let [find-spec (qs/find-spec query)
        with-clause (qs/with-clause query)
        inputs (qs/inputs query)
        find-vars (-> (qs/extract :vars find-spec) #_:vars)
        with-vars (-> (when with-clause (:variables with-clause)))
        find-lvars (sequence (comp cat (distinct))
                             [find-vars with-vars])
        input-vars (qs/input-vars inputs)

        input-lvars #_(:vars) input-vars
        input-src-vars #_(:src-vars) #_input-vars (qs/input-src-vars inputs)
        where-rule-vars (->where-rule-vars find-lvars input-lvars)
        where-rule (qs/->rule (gensym "where_")
                              input-src-vars
                              where-rule-vars
                              (qs/where-clauses query))]
    (assoc where-rule
           ::where-clause? true)))

(defn preprocess-where-with-rules
  "Handles the implicit '$ by checking the entire query and body of rules for any data patterns.
  If an implicit '$ is required, this makes it explicit, handles defining (or not) default
  src-vars for the where body and for the individual rules. Returns a (possibly modified)
  query-ast, a rule derived from the where body, and a map containing (possibly) default src vars;
  one for the where, one for the rules: {:keys [where rules]}."
  [query]
  (let [inputs (qs/inputs query)
        data-patterns (qs/data-patterns query)
        edb-required? (not-empty data-patterns)
        implict-$? (and edb-required? (empty? inputs))
        inputs (if implict-$? qs/implicit-inputs inputs)
        query (assoc-in query [:query :inputs :inputs] inputs)
        default-src-vars (when edb-required?
                           {:where (when (some #(= qs/implicit-src-var %) inputs)
                                     (src-lvar))
                            :rules (src-lvar)})
        query-rule (->where-rule query)]
    (-> query
        (update :rules conj query-rule)
        (assoc :query-rule query-rule
               :default-src-vars default-src-vars))))

;;;;;;;
;;;;;;;
;;;;;;;

(defn pred-merge
  "As we process the query, we build up maps containing information about the
  predicates used in the query: which of its variables are required, how many arguments
  are given to the predicate, and whether the predicate is extensional, evaluable,
  or a rule reference. When distinct instances of the predicate are encountered,
  the maps are merged: the resulting map should be the same, but with the union
  of the required-args data from the original two. In addition, equality is insisted
  for the types in the two maps and the argument counts in the two maps."
  [map-1 map-2]
  (when-not (= (:term-count map-1) (:term-count map-2))
    (raise-translation-error :invalid-form
                             (format "Invalid query form: %s takes %d terms in one place and %d terms in another."
                                     (:raw-ref map-1) (:term-count map-1) (:term-count map-2))
                             {:form (:raw-ref map-1),}))
  (when-not (= (:type map-1) (:type map-2))
    (raise-translation-error :invalid-form
                             (format "Invalid query form: %s cannot be both %s and %s."
                                     (:raw-ref map-1) (-> map-1 :type name) (-> map-2 :type name))
                             {:form (:raw-ref map-1)}))
  {:required (clojure.set/union (:required map-1) (:required map-2))
   :raw-ref (:raw-ref map-2)
   :term-count (:term-count map-1)
   :type (:type map-2)})

(defn pred-info->pred-gens
  "Pass in the map of predicate symbols to predicate information maps,
  and get out a map of predicate symbols to functions that generate the
  appropriate Predicate objects for the datalog engine."
  [sym->pred-info]
  (into {}
        (for [[sym {:keys [required term-count type]}] sym->pred-info]
          [sym (pred/predicate sym {:term-count term-count,
                                    :required (-> required sort vec),
                                    :type type})])))

(defn create-?src-vars
  [rule-srcs default-src-var clauses-ast where-clause?]
  (let [$->? ($->?src* rule-srcs)]
    {:rule-srcs (clojure.walk/postwalk-replace $->? rule-srcs)
     :default-src-var (if where-clause?
                        (get $->? '$ default-src-var) ;; If it's a where-clause, we're generating the top-level default
                        (get $->? default-src-var default-src-var))
     :clauses (clojure.walk/postwalk-replace $->? clauses-ast)}))

(defn insist-no-$s-remaining
  [rule-name clauses]
  #_(when-let [unresolved (not-empty (qs/extract u/src-var? clauses))]
    (raise-edb-error (format "Unable to resolve src-vars %s in rule '%s'."
                             unresolved
                             rule-name)
                     {:src-var unresolved,
                      :processed-clauses clauses})))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Clause Processing
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(declare process-rule)

(defmulti process-clause (fn [[clause-type] default-src-var datom-pred-sym] clause-type))

;; expression clauses are overarching on other clause types
(defmethod process-clause ::qs/expression-clause [[_ inner-clause] default-src-var datom-pred-sym]
  (process-clause inner-clause default-src-var datom-pred-sym))

(defmethod process-clause ::qs/data-pattern [[_ data-pattern] default-src-var datom-pred-sym]
  (let [src-var (if-let [src (:src-var data-pattern)]
                  src
                  default-src-var)]
    {:expression (list* datom-pred-sym :+ src-var (map second (:pattern data-pattern)))
     :rules {}, ;; no new rules were generated
     :sym->pred-info {} ;; no new predicate info
     :sym->evals {}}))

(defmethod process-clause ::qs/rule-expr
  [[_ rule-expr] default-src-var datom-pred-sym]
  (let [explicit-srcs (:src-vars rule-expr) ;; all src-vars in process-clauses should already be turned to ?src__ form.
        src-vars (if (empty? explicit-srcs)
                   (if (some? default-src-var)
                     ;; NOTE: IF there exists a default-src-var, then it WILL be passed.
                     ;; Corollary: all rules must accept at least one src-var if ANY extensional source
                     ;;            is used anywhere in the query or rules.
                     [default-src-var]
                     [])
                   explicit-srcs)
        expression
        (list* (:rule-name rule-expr) :+ (concat src-vars (map second (:rule-args rule-expr))))]
    {:expression expression
     :rules (), :sym->pred-info {}, :sym->evals {}}))

(def ^:private batch-fns #{#'fn/missing? #'fn/get-else})

(defmethod process-clause ::qs/pred-expr
  [[_ pred-expr] src-var datom-pred-sym]
  (let [pred-name (get-in pred-expr [:pred-expr :pred])]
    (if-let [f (ns-resolve sandbox/*sandbox-ns* pred-name)]
      (let [gsym (gensym (str pred-name "__"))
            args (map second (get-in pred-expr [:pred-expr :fn-args]))
            ->evaluable (if (contains? batch-fns f)
                          eval/batched-pred-fn->Evaluable
                          eval/pred-fn->Evaluable)]
        {:expression (list* gsym :+ args)
         :rules {}, ;; no additional rules were created
         :sym->pred-info {gsym {:required (set (range (count args)))
                                :raw-ref (str "predicate function '" pred-name "'")
                                :term-count (count args)
                                :type :evaluable}}
         :sym->evals {gsym (->evaluable f)}})
      (raise-translation-error :unresolved-sym
                               (format "PredExpr clause contained '%s'." pred-name)
                               {:fn-sym (pred-name)}))))

(defmethod process-clause ::qs/fn-expr
  [[_ fn-expr] src-var datom-pred-sym]
  (if-let [f (ns-resolve sandbox/*sandbox-ns* (get-in fn-expr [:fn-expr :fn]))]
    (let [gsym (gensym (str (get-in fn-expr [:fn-expr :fn]) "__"))
          binding-form  (:binding fn-expr)
          binding-vars (qs/binding-vars binding-form)
          ->evaluable (case (first binding-form)
                        :bind-scalar (if (contains? batch-fns f)
                                       eval/batched-scalar-bind-fn->Evaluable
                                       eval/scalar-bind-fn->Evaluable)
                        :bind-coll eval/coll-bind-fn->Evaluable
                        :bind-tuple eval/tuple-bind-fn->Evaluable
                        :bind-rel eval/relation-bind-fn->Evaluable)
          args (map second (get-in fn-expr [:fn-expr :fn-args]))
          all-terms (concat args binding-vars)] ;; (arg-var-1 arg-var-n bind-var-1 bind-var-n)
      {:expression (list* gsym :+ all-terms),
       :rules {}, ;; no additional rules were generated
       :sym->pred-info {gsym {:required (set (range (count args)))
                              :raw-ref (format "function '%s'" (:name fn-expr))
                              :term-count (count all-terms)
                              :type :evaluable}},
       :sym->evals {gsym (->evaluable f (count args))}})
    (raise-translation-error :unresolved-sym
                             (format "FnExpr clause contained '%s'." (:name fn-expr))
                             {:fn-sym (:name fn-expr)})))

(defn- process-or*-clause
  [sym [type clause] default-src-var datom-pred-sym]
  (let [src-var (or (-> clause :src-var) default-src-var) ;; use current default (if it exists) unless one is provided.
        src-var (when src-var [src-var])
        rule-vars (:rule-vars clause)
        rule-declarations (doall (for [[type subclause] (:clauses clause)]
                                   (do (case type
                                         :and-clause (qs/->rule sym
                                                                src-var
                                                                rule-vars
                                                                (:clauses subclause))
                                         :clause (qs/->rule sym
                                                            src-var
                                                            rule-vars
                                                            [subclause])))))

        rule-result-maps (doall (map #(process-rule % src-var datom-pred-sym)
                                     rule-declarations))
        recursive-rule-expressions (into () (mapcat :expressions) rule-result-maps)
        ;; consistency checked here.
        sym->pred-info (transduce (map :sym->pred-info)
                                  (partial merge-with pred-merge)
                                  rule-result-maps)
        sym->evals (transduce (mapcat :sym->evals) merge rule-result-maps)
        rule-vars (concat src-var (qs/flat-rule-vars rule-vars))]
    (cond-> {:rule-result-maps rule-result-maps
             :rule-vars rule-vars
             :recursive-rule-expressions recursive-rule-expressions
             :sym->pred-info sym->pred-info
             :sym->evals sym->evals}
      (some? src-var) (assoc :src-var src-var))))

(defmethod process-clause ::qs/or-clause
  [[_ or-clause :as whole-clause] src-var datom-pred-sym]
  (let [sym (gensym "or__")
        all-vars (qs/extract :vars whole-clause)
        non-src-vars (filter (complement lvar-is-src-var?) all-vars) #_(:vars all-vars)
        src-vars (filter lvar-is-src-var? all-vars)
        {:keys [rule-result-maps
                rule-vars
                recursive-rule-expressions
                sym->pred-info
                sym->evals]}(process-or*-clause sym
                                                [::qs/or-join-clause
                                                 (assoc or-clause
                                                        :rule-vars
                                                        (qs/->rule-vars
                                                         non-src-vars
                                                         src-vars))]
                                                src-var
                                                datom-pred-sym)
        ;; lvars required in or-clauses are the union of the required bindings for its subclauses,
        ;; intersected with the set of lvars that must unify with the rest of the query
        ;; Hence the following:
        subrule-required (->> (for [result-map rule-result-maps]
                                (keep-indexed (fn [idx lvar] (when ((:subclause-required result-map) lvar) idx))
                                              rule-vars))
                              (into #{} cat))
        ;; rule-vars (qs/flat-rule-vars rule-vars)
        expression (list* sym :+ rule-vars)
        sym-info {sym {:required subrule-required
                       :raw-ref (str "an or-clause") ;; <-- overwrites the generic 'rule' ref generated by process-rule
                       :term-count (count rule-vars) ;; inc because src-var
                       :type :rule}}]
    {:expression expression,
     :rules recursive-rule-expressions,
     :sym->pred-info (merge-with pred-merge
                                 sym->pred-info
                                 sym-info), ;; <-- in Datalog it's just a rule.
     :sym->evals sym->evals}))

(defmethod process-clause ::qs/or-join-clause
  [or-join-clause default-src-var datom-pred-sym]
  (let [sym (gensym "orjoin__")
        {:keys [recursive-rule-expressions
                sym->pred-info
                rule-vars
                sym->evals]} (process-or*-clause sym
                                                 or-join-clause
                                                 default-src-var
                                                 datom-pred-sym)
        rule-lvars rule-vars #_(concat src-var rule-vars #_(qs/flat-rule-vars rule-vars)#_(s/unform ::qs/rule-vars rule-vars))]
    {:expression (list* sym :+ rule-lvars),
     :rules recursive-rule-expressions,
     :sym->pred-info (merge-with pred-merge
                                 sym->pred-info
                                 {sym {:raw-ref (str "an or-join clause") ;; <-- overwrites the generic 'rule' ref generated by process-rule
                                       :term-count (count rule-lvars)
                                       :type :rule}}), ;; <-- in Datalog it's just a rule.
     :sym->evals sym->evals}))

(defmethod process-clause ::qs/not-clause
  [[_ not-clause :as whole-clause] src-var datom-pred-sym]
  (let [all-vars (qs/extract :vars whole-clause)]
    ;; Delegate! NotClause special case of NotJoinClause.
    (process-clause [::qs/not-join-clause
                     (assoc not-clause :variables #_(:vars) all-vars)]
                    src-var
                    datom-pred-sym)))

(defmethod process-clause ::qs/not-join-clause
  [[_ not-join-clause] default-src-var datom-pred-sym]
  (let [sym (gensym "not__")
        ;; explicit, else use default (if it exists)
        src-var (or (-> not-join-clause :src-var) default-src-var)
        rule-args (:variables not-join-clause)
        ;; The Datalog engine should be smart enough that require? = 'true'
        ;; here isn't necessary, but dotted i's and all that
        rule-vars (qs/->rule-vars nil rule-args)

        not-subrule (qs/->rule sym
                               (if (some? src-var) [src-var] [])
                               rule-vars
                               (:clauses not-join-clause))

        ;; delegate to rule processing
        {:keys [expressions,
                sym->pred-info,
                sym->evals]} (process-rule not-subrule src-var datom-pred-sym)
        expression (list* sym
                          ;; indicates negated clause
                          :-
                          ;; it's possible to have a not-join clause occur inside
                          ;; a rule that has no default src-var defined.
                          (cond->> rule-args (some? src-var) (cons src-var)))]
    {:expression expression
     :rules expressions,
     :sym->pred-info sym->pred-info
     :sym->evals sym->evals}))


(defn process-clauses
  "Recursive depth-first pass of ast, generating a map with the keys :clauses,
  :rules, :sym->pred-info, and :sym->evals. :sym->pred-info is a map from predicate
  symbols to consistency-verified information maps (see pred-merge) for each predicate.
  :sym->evals is a map from predicate symbols to appropriate Evaluable objects. :rules
  is just a collection of expressions encoding all rule declarations created while traversing
  (for example, by processing an orjoin): [(rule-pred & args) [& clauses]]."
  [clauses default-src-var datom-pred-sym]
  (loop [[clause & clauses] clauses,
         expressions [],
         cumulative-rules (),
         s->pi {},
         s->ev {}]
    (if clause
      (let [{:keys [expression,
                    rules,
                    sym->pred-info,
                    sym->evals]} (process-clause clause default-src-var datom-pred-sym)]
        (recur clauses
               (conj expressions expression)
               (into cumulative-rules rules)
               (merge s->pi sym->pred-info)
               (merge s->ev sym->evals)))
      {:clauses expressions,
       :rules cumulative-rules,
       :sym->pred-info s->pi,
       :sym->evals s->ev})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn process-rule [rule default-src-var datom-pred-sym]
  (let [rule-name (qs/rule-name rule)
        rule-srcs (qs/rule-srcs rule)
        where-clause? (::where-clause? rule)
        default-src-var (cond
                          ;; if the rule-head contains multiple src-vars, its body has no default implicit src-var.
                          (and (not where-clause?)
                               (> (count rule-srcs) 1))
                          nil
                          ;; if a rule-head contains one explicit src-var, that src-var is default and implicit for the body.
                          (= 1 (count rule-srcs))
                          (first rule-srcs)
                          ;; default src var remains unchanged. It might be nil -- we check later.
                          :else
                          default-src-var)
        rule-srcs (if (and (empty? rule-srcs)  ;; if there were no explicit src-vars...
                           (some? default-src-var)) ;; ... but we inherit an implicit src-var...
                    [default-src-var] ;; datalog requires it be explicit
                    rule-srcs);; otherwise leave them alone.

        {:keys [rule-srcs default-src-var clauses] :as res}
        (create-?src-vars rule-srcs default-src-var (:clauses rule) where-clause?)

        _ (insist-no-$s-remaining rule-name clauses)

        rule-vars (concat (vec rule-srcs) (qs/all-rule-vars rule))
        required-var-set (into #{} (concat rule-srcs (qs/required-rule-vars rule)))
        required-var-idxs (set (keep-indexed
                                (fn [idx x] (when (contains? required-var-set x) idx))
                                rule-vars))
        consequent (apply list rule-name :+ rule-vars)

        {:keys [clauses,
                rules,
                sym->pred-info,
                sym->evals]} (process-clauses clauses default-src-var datom-pred-sym)
        sym->pred-info (assoc sym->pred-info
                              rule-name
                              {:term-count (count rule-vars)
                               :raw-ref (str "the rule '" rule-name "'")
                               :required required-var-idxs
                               :type :rule})
        subclause-required (for [[sym _ & vars] clauses
                                 :let [required (get-in sym->pred-info [sym :required])]
                                 :when required
                                 idx required] (nth vars idx))
        expression [consequent (vec clauses)]
        ]
    {:expressions (cons expression rules),
     :subclause-required (set subclause-required), ;; <-- for the sake of orjoin clauses.
     :sym->pred-info sym->pred-info,
     :sym->evals sym->evals}
    ))



(defn default-src-var [query rule]
  (if (::where-clause? rule)
    (get-in query [:default-src-vars :where])
    (get-in query [:default-src-vars :rules])))

(defn process-query [query]
  (let [datom-pred-sym (gensym "datom_")]
    (loop [[rule & rules] (qs/rules query)
           rule-expressions ()
           s->pi {}
           s->ev {}]
      (let [{:keys [expressions
                    sym->pred-info
                    sym->evals]}
            (process-rule rule (default-src-var query rule) datom-pred-sym)

            sym->pred-info (merge-with pred-merge s->pi sym->pred-info)
            sym->evals (merge s->ev sym->evals)
            expressions (into rule-expressions expressions)]
        (if (not-empty rules)
          (recur rules expressions sym->pred-info sym->evals)
          {:query query
           :datom-pred datom-pred-sym
           :rule-expressions expressions,
           :sym->pred-gens (pred-info->pred-gens sym->pred-info)
           :sym->evals sym->evals})))))
