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

(ns eva.query.dialect.spec
  (:refer-clojure :exclude [+ * and or cat])
  (:require [clojure.spec.alpha :as s
             :refer [+ cat alt ? * spec and or]]
            [clojure.string :refer [starts-with?]]
            [eva.query.dialect.translation.error :as err]
            [expound.alpha :as exp]
            [clojure.core.unify :as u]))

(def expound-printer (exp/custom-printer {:show-valid-values? false
                                          :print-specs? false}))

(defn conform! [spec x]
  (binding [s/*explain-out* expound-printer]
    (let [conformed (s/conform spec x)]
      (if (s/invalid? conformed)
        (err/raise-translation-error
         :invalid-form
         (s/explain-str spec x)
         #_(str "failed to conform " spec ", see ex-data")
         {:spec spec
          :explanation (s/explain-str spec x)
          :data (s/explain-data spec x)
          :value x})
        conformed))))

(defmacro deferrspec
  ([kw spec]
   `(do (s/def ~kw ~spec)
        (exp/defmsg ~kw (name ~kw))))
  ([kw spec err-msg]
   `(do (s/def ~kw ~spec)
        (exp/defmsg ~kw (name ~kw)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Pull grammar
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;; pattern-data-literal = [attr-spec+]
(deferrspec ::pattern-data-literal (spec (+ ::attr-spec)))

;; attr-spec = attr-name | wildcard | map-spec | attr-expr
(deferrspec ::attr-spec (alt :attr-name ::attr-name
                             :wildcard ::wildcard
                             :map-spec ::map-spec
                             :attr-expr ::attr-expr))

;; attr-name = an edn keyword that names an attr
(deferrspec ::attr-name #_keyword?
  (or :db/id    #{:db/id}
      :rev-attr (and keyword? #(starts-with? (name %) "_"))
      :fwd-attr keyword?))

;; wildcard = "*" or '*'
(deferrspec ::wildcard #{"*" '*})

;; map-spec = { ((attr-name | limit-expr) (pattern-data-literal | recursion-limit))+ }
(deferrspec ::map-spec (s/map-of (or :attr-name ::attr-name
                                     :limit-expr ::limit-expr)
                                 (or :pattern ::pattern-data-literal
                                     :recursion-limit ::recursion-limit)
                                 :conform-keys true
                                 :min-count 1))

;; attr-expr = limit-expr | default-expr
(deferrspec ::attr-expr (alt :limit-expr ::limit-expr
                             :default-expr ::default-expr))

;; limit-expr = [("limit" | 'limit') attr-name (positive-number | nil)]
(deferrspec ::limit-expr (spec (cat :limit-expr #{"limit" 'limit}
                                    :attr-name ::attr-name
                                    :limit (s/nilable pos-int?))))

;; default-expr = [("default" | 'default') attr-name any-value]
(deferrspec ::default-expr (spec (cat :default-expr #{"default" 'default}
                                      :attr-name ::attr-name
                                      :value any?)))

;; recursion-limit = positive-number | '...'
(deferrspec ::recursion-limit (or :limit pos-int?
                                  :limit #{'... "..."}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Query grammar
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Misc

;; rules-var = the symbol "%"
(deferrspec ::rules-var #{'%})

;; src-var = symbol starting with "$"
(deferrspec ::src-var (and symbol? #(starts-with? % "$")))

;; variable = symbol starting with "?"
(deferrspec ::variable (and symbol? #(starts-with? % "?")))

;; constant = any non-variable data literal
(deferrspec ::constant (and #(not (s/valid? ::variable %))
                            #(not (instance? java.util.List %))))

;; fn-arg = (variable | constant | src-var)

(deferrspec ::fn-arg (alt :variable ::variable
                          :constant ::constant
                          :src-var ::src-var))

(deferrspec ::unbound #{'_})

;; plain-symbol = symbol that does not begin with "$" or "?" and is not "%" or "_"
(deferrspec ::plain-symbol (and symbol?
                                #(not (s/valid? ::src-var %))
                                #(not (s/valid? ::variable %))
                                #(not (s/valid? ::rules-var %))
                                #(not (s/valid? ::unbound %))))

(deferrspec ::unqualified-plain-symbol (and ::plain-symbol
                                            #(= nil (namespace %))))

;; pattern-var = unqualified-plain-symbol
(deferrspec ::pattern-var ::unqualified-plain-symbol)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Datalog

;; datalog = [find-spec with-clause? inputs? where-clauses?]
(deferrspec ::datalog (cat :find-spec     #_(?) ::find-spec
                           :with-clause   (? ::with-clause)
                           :inputs        (? ::inputs)
                           ;; TODO: depending on details, we might not actually need to omit the where clause
                           :where-clauses #_(?) ::where-clauses))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Find

;; find-spec = (':find' | ':select') (find-rel | find-coll | find-tuple | find-scalar)
(deferrspec ::find-spec (cat :find #{:find :select}
                             :spec (alt :find-rel    ::find-rel
                                        :find-coll   ::find-coll
                                        :find-tuple  ::find-tuple
                                        :find-scalar ::find-scalar)))

;; find-rel = find-elem+
(deferrspec ::find-rel (+ ::find-elem))

;; find-coll = [find-elem '...']
(deferrspec ::find-coll (spec (cat :find-elem ::find-elem
                                   :elipsis #{'...})))

;; find-scalar = find-elem '.'  [:find ?x . :in]
(deferrspec ::find-scalar (cat :find-elem ::find-elem
                               :dot #{'. "."}))

;; find-tuple = [find-elem+]
(deferrspec ::find-tuple (spec (cat :find-elems (s/+ ::find-elem))))

;; find-elem = (variable | pull-expr | aggregate)
(deferrspec ::find-elem (alt :pull-expr ::pull-expr
                             :aggregate ::aggregate
                             :variable  ::variable))

;; pull-expr = ['pull' variable pattern]
(deferrspec ::pull-expr (spec (cat :pull #{'pull "pull"}
                                   :src-var (s/? ::src-var)
                                   :variable ::variable
                                   :pattern ::pattern)))

;; pattern = (input-name | pattern-data-literal)
(deferrspec ::pattern (alt :input-name ::pattern-var
                           :pattern-data-literal ::pattern-data-literal))

;; aggregate = [aggregate-fn-name fn-arg+]
(deferrspec ::aggregate (spec (cat :aggregate-fn-name ::plain-symbol
                                   :fn-args (+ ::fn-arg))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; With

;; with-clause = ':with' variable+
(deferrspec ::with-clause (cat :with #{:with}
                               :variables (+ ::variable)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; In

;; inputs = ':in' (src-var | binding | pattern-var | rules-var)+
(deferrspec ::inputs (cat :in #{:in}
                          :inputs (+ (alt :src-var ::src-var
                                          :binding ::binding
                                          :pattern-var ::pattern-var
                                          :rules-var ::rules-var))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Where

;; where-clauses = ':where' clause+
(deferrspec ::where-clauses (cat :where #{:where}
                                 :clauses (+ ::clause)))

;; clause = (not-clause | not-join-clause | or-clause | or-join-clause | expression-clause)
(deferrspec ::clause (alt ::not-clause        ::not-clause
                          ::not-join-clause   ::not-join-clause
                          ::or-clause         ::or-clause
                          ::or-join-clause    ::or-join-clause
                          ::expression-clause ::expression-clause))

;; not-clause = [ src-var? 'not' clause+ ]
(deferrspec ::not-clause (spec (cat :src-var (? ::src-var)
                                    :not #{'not}
                                    :clauses (+ ::clause))))

;; not-join-clause = [ src-var? 'not-join' [variable+] clause+ ]
(deferrspec ::not-join-clause (spec (cat :src-var (? ::src-var)
                                         :not-join #{'not-join}
                                         :variables (spec (+ ::variable))
                                         :clauses (+ ::clause))))

;; and-clause = [ 'and' clause+ ]
(deferrspec ::and-clause (spec (cat :and #{'and}
                                    :clauses (+ ::clause))))

;; or-clause = [ src-var? 'or' (clause | and-clause)+]
(deferrspec ::or-clause (spec (cat :src-var (? ::src-var)
                                   :or #{'or}
                                   :clauses (+ (alt :and-clause ::and-clause
                                                    :clause ::clause)))))

;; rule-vars = [variable+ | ([variable+] variable*)]
(deferrspec ::rule-vars (alt :variables         (+ ::variable)
                             :required-bindings (cat :required-variables (spec (+ ::variable))
                                                     :variables (* ::variable))))



;; or-join-clause = [ src-var? 'or-join' rule-vars (clause | and-clause)+ ]
(deferrspec ::or-join-clause (spec (cat :src-var (? ::src-var)
                                        :or-join #{'or-join}
                                        :rule-vars (spec ::rule-vars)
                                        :clauses (+ (alt :and-clause ::and-clause
                                                         :clause ::clause)))))

;; pred-expr = [ [pred fn-arg+] ]
(deferrspec ::pred-expr (spec (cat :pred-expr (spec (cat :pred ::plain-symbol
                                                         :fn-args (+ ::fn-arg))))))

;; fn-expr = [ [fn fn-arg+] binding]
(deferrspec ::fn-expr (spec (cat :fn-expr (spec (cat :fn ::plain-symbol
                                                     :fn-args (+ ::fn-arg)))
                                 :binding ::binding)))

;; expression-clause = (data-pattern | pred-expr | fn-expr | rule-expr)
;; NOTE: we have to evaluate rule-expr before data-pattern to avoid ambiguity
;;       with plain-symbol constants in data patterns.
(deferrspec ::expression-clause (alt ::rule-expr    ::rule-expr
                                     ::pred-expr    ::pred-expr
                                     ::fn-expr      ::fn-expr
                                     ::data-pattern ::data-pattern))

;; rule-expr = [ src-var? rule-name (variable | constant | '_')+]
(deferrspec ::rule-expr (spec (cat :src-vars (* ::src-var)
                                   :rule-name ::plain-symbol
                                   :rule-args (+ (alt :variable ::variable
                                                      :unbound  ::unbound
                                                      :constant ::constant)))))

;; data-pattern = [ src-var? (variable | constant | '_')+ ]
(deferrspec ::data-pattern (spec (cat :src-var (? ::src-var)
                                      :pattern (+ (alt :variable ::variable
                                                       :unbound  ::unbound
                                                       :constant ::constant)))))

;; binding = (bind-scalar | bind-tuple | bind-coll | bind-rel)
(deferrspec ::binding (alt :bind-scalar ::bind-scalar
                           :bind-tuple  ::bind-tuple
                           :bind-coll   ::bind-coll
                           :bind-rel    ::bind-rel))

;; bind-scalar = variable
(deferrspec ::bind-scalar ::variable)

;; bind-tuple = [ (variable | '_')+]
(deferrspec ::bind-tuple (spec (+ (alt :variable ::variable
                                       :unbound ::unbound))))

;; bind-coll = [variable '...']
(deferrspec ::bind-coll (spec (and (cat :variable ::variable
                                        :elipsis #{'...}))))

;; bind-rel = [ [(variable | '_')+] ]
(deferrspec ::bind-rel (and (s/coll-of (+ (alt :variable ::variable
                                               :unbound ::unbound)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Rules

;; rule-head = [src-vars* rule-name rule-vars]
(deferrspec ::rule-head (spec (cat :src-vars  (* ::src-var)
                                   :rule-name ::unqualified-plain-symbol
                                   :rule-vars ::rule-vars)))

(deferrspec ::rule (spec (cat :rule-head ::rule-head
                              :clauses  (+ ::clause))))

;; rule = [ [rule-head clause+]+ ]
(deferrspec ::rules (+ ::rule))

(deferrspec ::query (spec (cat :query (spec ::datalog)
                               :rules (s/? (spec ::rules)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; Helpers on conformed specs

(defn find-spec [query] (get-in query [:query :find-spec :spec]))
(defn with-clause [query] (get-in query [:query :with-clause]))
(defn inputs [query] (get-in query [:query :inputs :inputs]))
(defn where-clauses [query] (get-in query [:query :where-clauses :clauses]))

(defn rules [query] (get query :rules))

(defn rule-name [rule] (get-in rule [:rule-head :rule-name]))
(defn rule-srcs [rule] (get-in rule [:rule-head :src-vars] []))

(defn flat-rule-vars [rule-vars]
  (let [[type data] rule-vars]
    (case type
      :variables data
      :required-bindings
      (concat (:required-variables data) (:variables data))
      nil nil)))

(defn all-rule-vars [rule]
  (flat-rule-vars (get-in rule [:rule-head :rule-vars])))

(defn required-rule-vars [rule]
  (let [[type data] (get-in rule [:rule-head :rule-vars])]
    (case type
      :variables nil
      :required-bindings (:required-variables data)
      nil nil)))

(defn first= "produce first = predicate" [x]
  (fn [y]
    (= x (clojure.core/and (seqable? y)
                           (first y)))))

(def implicit-src-var '[:src-var $])
(def implicit-inputs [implicit-src-var])

(defmulti binding-vars first)

(defmethod binding-vars :bind-scalar [[_ var]] [var])
(defmethod binding-vars :bind-coll   [[_ var]] [(:variable var)])
(defmethod binding-vars :bind-tuple  [[_ coll-of-var-or-unbound]] (map second coll-of-var-or-unbound))
(defmethod binding-vars :bind-rel    [[_ coll-of-var-or-unbound]] (map second (first coll-of-var-or-unbound)))

(defn ->rule-vars [optional-vars required-vars]
  (if (not-empty required-vars)
    [:required-bindings
     {:required-variables required-vars
      :variables optional-vars}]
    [:variables optional-vars]))

(defn ->rule [rule-name src-vars rule-vars clauses]
  (cond-> {:rule-head {:rule-name rule-name
                       :rule-vars rule-vars}
           :clauses clauses}
    (not-empty src-vars)
    (assoc-in [:rule-head :src-vars] src-vars)))

(defn extract* [pred form]
  (sequence (comp (distinct)
                  (filter pred))
            (tree-seq seqable? seq form)))

;; Data pattern extraction
(defn data-patterns [query]
  (extract* #(clojure.core/and
              (seqable? %)
              (= ::data-pattern (first %)))
            query))

;; Variable extraction

(defn extract [type form]
  (case type
    :vars (extract* (every-pred u/lvar? (complement u/ignore-variable?)) form)
    :src-vars (extract* #(s/valid? ::src-var %) form)))

(defn input-vars [input]
  (extract :vars input))

(defn input-src-vars [input]
  (extract :src-vars input))

(defn rules-idx [conformed-query]
  (->> conformed-query
       :inputs
       :inputs
       (keep-indexed #(when (= :rules-var (first %2)) %1))
       first))
