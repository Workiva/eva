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

(ns eva.query.datalog.qsqr.state
  (:require [eva.query.datalog.protocols :as p]
            [eva.query.datalog.predicate :as pred]
            [eva.query.datalog.qsqr.protocols :as qp]
            [eva.query.util :refer [simple-unify simple-unifications]]
            [eva.query.datalog.error :refer [raise-bindings]]
            [recide.sanex :as sanex]
            [eva.query.util :as qutil]
            [clojure.core.unify :as u]
            [eva.error :refer [insist]]
            [morphe.core :as d]
            [ichnaie.core :refer [traced]]))

;; "The state memorized by the algorithm is still a couple < Q, R >, where Q is
;; a set of generalized queries and R is a set of derived relations together with
;; their current values."

;; :derived structure
;; { relation #{ constants }}

;; :rule-log, extension-log, evaluation-log structures
;; { predicate-symbol { decoration { lvar-signature bindings}}}

;; :bindings structure
;; #{ unifier, unifier, unifier ... } (unifier = { ?a ?b, ?b c, ... })

(defn- log-key
  [query]
  (cond (p/rule? query) :rule-log
        (p/extensional? query) :extension-log
        (p/evaluable? query) :evaluation-log))

(defn- signature
  "Returns a 'signature' of this particular query, as a description of
  equivalence between its various terms. Examples:
  (pred a b c) gives [0 1 2].
  (pred a b a) gives [0 1 0]
  (pred a b c d a c b) gives [0 1 2 3 0 2 1]"
  [query]
  (loop [i 0
         seen {}
         [term & terms] (p/terms query)
         res []]
    (let [seen (if (contains? seen term)
                 seen
                 (assoc seen term i))
          res (conj res (seen term))]
      (if terms
        (recur (inc i) seen terms res)
        res))))

(defrecord State
    [rule-log extension-log evaluation-log derived bindings rule-selector pred-selector]
  qp/State
  (rule-log [_] rule-log)
  (rule-log [this qs] (assoc this :rule-log qs))
  (extension-log [_] extension-log)
  (extension-log [this es] (assoc this :extension-log es))
  (evaluation-log [_] evaluation-log)
  (evaluation-log [this es] (assoc this :evaluation-log es))
  (add-query [this query]
    (when-not (satisfies? p/Decorated query)
      (throw (IllegalArgumentException. "Only generalized predicates can be added to the state.")))
    (let [record (fn [old]
                   (if-not old
                     (p/bound-terms query)
                     (into old (p/bound-terms query))))]
      (update-in this
                 [(log-key query) (p/sym query) (p/decoration query) (signature query)]
                 record)))
  (derived [_] derived)
  (bindings [_] bindings)
  (bindings [this x] (assoc this :bindings x))
  (reset-bindings [this] (assoc this :bindings #{{}}))
  (select-antecedent [state predicates] (pred-selector state predicates))
  (select-rule [state query rules] (rule-selector state query rules)))

(defn- select-first
  [f coll]
  (loop [current (first coll)
         tail (next coll)
         res []]
    (if (f current)
      [current (vec (concat res tail))]
      (if (nil? tail)
        nil
        (recur (first tail) (next tail) (conj res current))))))

(defn rule-required-met?
  [decoration rule]
  (clojure.set/subset? (set (p/required-bindings (p/consequent rule))) decoration))

(defn pred-required-met?
  [bindings pred]
  (cond (p/negated? pred)
        (clojure.set/subset? (set (filter u/lvar? (p/terms pred))) (-> bindings first keys set))

        (p/extensional? pred)
        (or (empty? (p/required-bindings pred))
            (let [terms (map #(nth (p/terms pred) %) (p/required-bindings pred))
                  representative (first bindings)]
              (every? (some-fn (complement u/lvar?) #(contains? representative %)) terms)))

        :else
        (or (empty? (p/required-bindings pred))
            (let [terms (map #(nth (p/terms pred) %) (p/required-bindings pred))
                  representative (first bindings)]
              (every? (some-fn (complement u/lvar?) #(contains? representative %)) terms)))))

(defn default-pred-selector
  [state preds]
  (or (select-first (partial pred-required-met? (qp/bindings state))
                    preds)
      (raise-bindings "no predicate could be selected."
                      {:predicates preds,
                       :bindings (-> state qp/bindings first keys),
                       ::sanex/sanitary? false}))) ;; customer data

(defn default-rule-selector
  [_ query rules]
  (let [decoration (set (p/decoration query))
        req-met? (partial rule-required-met? decoration)]
    (or (select-first req-met? rules)
        (raise-bindings "no rule could be selected."
                        {:rules rules,
                         ::sanex/sanitary? false}))))

(def init-state (->State {} {} {} {} #{{}} default-rule-selector default-pred-selector))

(d/defn ^{::d/aspects [traced]} extract-derived
  [state predicate]
  (let [terms (p/terms predicate)]
    (distinct (for [bindings (qp/bindings state)]
                (replace (u/flatten-bindings bindings) terms)))))

(d/defn ^{::d/aspects [traced]} update-derived
  [state predicate derived]
  (update-in state [:derived (p/sym predicate)] (fnil into #{}) derived))

(defn trim-bindings
  [state generalized-query]
  (let [relevant-lvars (filter u/lvar? (p/terms generalized-query))
        bindings (qp/bindings state)]
    (qp/bindings state
                 (into #{}
                       (map #(select-keys % relevant-lvars))
                       bindings))))

(defn join
  "Performs a join in 'linear time' O(input + output). merge should be a
  function that actually performs the join on each pair of items from
  each cross-product determined by the discriminator. :)" ;; TODO: better words
  ([a b disc merge] (join a identity b identity disc merge))
  ([a disc-a b disc-b merge] (join a disc-a b disc-b identity merge))
  ([a proj-a b proj-b disc merge]
   (let [a-groups (group-by (comp disc proj-a) a)
         b-groups (group-by (comp disc proj-b) b)]
     (for [k (keys a-groups)
           :let [as (get a-groups k)
                 bs (get b-groups k)]
           :when bs
           a as
           b bs
           :let [merged (merge a b)]
           :when (some? merged)]
       merged))))

(defn join-with-batched-merge-fn
  "Performs a join in 'linear time' O(input + output). Like `join`, but
  takes a merge-fn that operates on the full sets of items involved in
  each cross-product of the join."
  ([a b disc cross-merge] (join-with-batched-merge-fn a identity b identity disc cross-merge))
  ([a disc-a b disc-b cross-merge] (join-with-batched-merge-fn a disc-a b disc-b identity cross-merge))
  ([a proj-a b proj-b disc cross-merge]
   (let [a-groups (group-by (comp disc proj-a) a)
         b-groups (group-by (comp disc proj-b) b)]
     (reduce into #{}
             (for [k (keys a-groups)
                  :let [as (get a-groups k)
                        bs (get b-groups k)]
                  :when bs]
                (cross-merge as bs))))))

(defn add-unifiable
  [bindings predicate derived]
  (let [terms (p/terms predicate)
        exemplar (first bindings)]
    (cond (empty? derived)
          #{}
          :else
          (let [lvars (sequence (filter exemplar) terms)
                indices (set (keep-indexed #(when (exemplar %2) %) terms))]
            (if (empty? indices)
              (simple-unifications terms bindings derived)
              (let [bindings-disc (if (empty? lvars)
                                    (constantly ())
                                    (apply juxt (map #(fn [m] (get m %)) lvars)))
                    derived-disc (fn [v] (keep-indexed #(when (indices %) %2) v))]
                (join-with-batched-merge-fn bindings
                                            bindings-disc
                                            derived
                                            derived-disc
                                            (partial simple-unifications terms))))))))

(d/defn ^{::d/aspects [traced]} remove-unifiable
  "Remove any unification-maps from bindings that are consistent
  with any currently derived relations for this predicate."
  [bindings predicate derived]
  (let [terms (p/terms predicate)]
    (into #{}
          (remove (fn [uni-map] (some (partial simple-unify terms uni-map) derived))) ;; TODO: could benefit from better traversal algorithm
          bindings)))

(defn conjunct-bindings
  "Updates the bindings stored in state using current derived relations."
  [state predicate]
  (let [relevant-derived (get (:derived state) (p/sym predicate))]
    (update state :bindings
            (if (p/negated? predicate) remove-unifiable add-unifiable)
            predicate
            relevant-derived)))

(defn predicate->generalized
  "Takes a Predicate and returns a GeneralizedPredicate incorporating information
  from the set of bindings."
  [predicate bindings-set]
  (let [terms (p/terms predicate)
        representative (first bindings-set)
        decoration (keep-indexed #(when (or (contains? representative %2)
                                            (not (u/lvar? %2)))
                                    %)
                                 terms)
        bindings (into #{}
                       (comp (map #(replace % terms))
                             (map (partial remove u/lvar?)))
                       bindings-set)]
    (pred/->GeneralizedPredicate predicate decoration bindings bindings-set)))

(defn novel-generalization ;; TODO: Doesn't check full subsumption of a query by a more general
  "Returns nil if the generalized gen-pred contains nothing novel given the execution
  state; otherwise, this returns the gen-pred with any previously-run bindings removed."
  [state gen-pred] ;; TODO: make sure this takes into account distinct data sources
  (if-let [prev (get-in state [(log-key gen-pred)
                               (p/sym gen-pred)
                               (p/decoration gen-pred)
                               (signature gen-pred)])]
    (when-let [novelty (-> (p/bound-terms gen-pred)
                           (clojure.set/difference prev)
                           (not-empty))]
      (p/bound-terms gen-pred novelty))
    gen-pred))
