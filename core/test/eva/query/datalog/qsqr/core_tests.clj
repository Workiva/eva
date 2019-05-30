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

(ns eva.query.datalog.qsqr.core-tests
  (:require [eva.query.datalog.qsqr.core :refer :all]
            [eva.query.datalog.predicate :as pred]
            [eva.query.datalog.rule :as rule]
            [eva.query.datalog.edb :as edb]
            [eva.query.datalog.evaluable :as eval]
            [eva.query.datalog.program :as prog]
            [clojure.test :refer :all])
  (:import [eva.error.v1 EvaException]))

(deftest unit:sgc-small
  (let [par-edb (edb/coll-of-tuples->EDB '[[john henry] [henry mary] [george jim] [jim mary]])
        eq-edb (edb/coll-of-tuples->EDB '[[john john] [henry henry] [mary mary] [george george] [jim jim]])
        sgc (pred/rule-predicate 'sgc {:term-count 2})
        eq (pred/extensional-predicate 'eq {:term-count 2})
        par (pred/extensional-predicate 'par {:term-count 2})
        rules [(rule/rule (sgc '[?a ?b])
                          [(eq '[?a ?b])])
               (rule/rule (sgc '[?a ?b])
                          [(par '[?a ?a1])
                           (sgc '[?a1 ?b1])
                           (par '[?b ?b1])])]
        program (prog/program :edbs {'eq eq-edb, 'par par-edb},
                              :rules rules)
        q (sgc '[john ?x])]
    (is (= '#{[john john] [john george]} (query program q)))))

(deftest unit:appendix
  (let [p-edb (edb/coll-of-tuples->EDB [[:c :d] [:b :c] [:c :b]])
        r-edb (edb/coll-of-tuples->EDB [[:d :e]])
        q-edb (edb/coll-of-tuples->EDB [[:e :a] [:a :i] [:i :o]])
        edbs {'p p-edb, 'r r-edb, 'q q-edb}
        n (pred/rule-predicate 'n {:term-count 2})
        r (pred/extensional-predicate 'r {:term-count 2})
        p (pred/extensional-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        s (pred/rule-predicate 's {:term-count 1})
        rules [(rule/rule (n '[?x ?y])
                          [(r '[?x ?y])])
               (rule/rule (n '[?x ?y])
                          [(p '[?x ?z])
                           (n '[?z ?w])
                           (q '[?w ?y])])
               (rule/rule (s '[?x])
                          [(n [:c '?x])])]
        program (prog/program :edbs edbs,
                              :rules rules)
        q (s '[?x])]
    (is (= #{[:a] [:o]}
           (query program q)))))

(deftest unit:example-3-1
  (let [q-edb (edb/coll-of-tuples->EDB [[:a :b] [:a :c] [:b :d] [:c :e] [:d :f] [:e :g]])
        edbs {'q q-edb}
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])])]
        program (prog/program :edbs edbs, :rules rules)
        q (p [:a '?x])]
    (is (= #{[:a :b] [:a :c] [:a :d] [:a :e] [:a :f] [:a :g]}
           (query program q)))))

(deftest unit:example-3-1-with-predicate-function
  (let [q-edb (edb/coll-of-tuples->EDB '[[a b] [a c] [b d] [c e] [d f] [e g]])
        edbs {'q q-edb}
        evaluators {'= (eval/pred-fn->Evaluable (fn [a b] (= a b)))}
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        = (pred/evaluable-predicate '= {:term-count 2 :required [0 1]})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])
                           (= '[?x f])])]
        program (prog/program :edbs edbs,
                              :evaluators, evaluators,
                              :rules rules)
        q (p '[a ?x])]
    (is (= '#{[a c] [a b]}
           (query program q)))))

(deftest unit:example-3-1-with-scalar-binding-function
  (let [q-edb (edb/coll-of-tuples->EDB '[[a b] [a c] [b d] [c e] [d f] [e g]])
        edbs {'q q-edb}
        evaluators {'id (eval/scalar-bind-fn->Evaluable (fn [a] a) 1)}
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        id (pred/evaluable-predicate 'id {:term-count 2 :required [0]})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])
                           (id '[?x f])])]
        program (prog/program :edbs edbs,
                              :evaluators, evaluators,
                              :rules rules)
        q (p '[a ?x])]
    (is (= '#{[a c] [a b]}
           (query program q)))))

(deftest unit:example-3-1-with-collection-binding-function
  (let [edbs {'q (edb/coll-of-tuples->EDB [[1 2] [1 3] [2 4] [3 5] [4 6] [5 7]])}
        evaluators {'range (eval/coll-bind-fn->Evaluable (fn [a] (range a)) 1)}
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 2})
        range (pred/evaluable-predicate 'range {:term-count 2 :required [0]})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])])
               (rule/rule (r '[?x ?y])
                          [(p '[1 ?x])
                           (range '[?x ?y])])]
        program (prog/program :edbs edbs,
                              :evaluators, evaluators,
                              :rules rules)
        q (r '[?x ?y])]
    (is (= #{[2 1] [2 0]
             [3 2] [3 1] [3 0]
             [4 3] [4 2] [4 1] [4 0]
             [5 4] [5 3] [5 2] [5 1] [5 0]
             [6 5] [6 4] [6 3] [6 2] [6 1] [6 0]
             [7 6] [7 5] [7 4] [7 3] [7 2] [7 1] [7 0]}
           (query program q)))))

(deftest unit:example-3-1-with-tuple-binding-function
  (let [edbs {'q (edb/coll-of-tuples->EDB [[1 2] [1 3] [2 4] [3 5] [4 6] [5 7]])}
        evaluators {'bizarro (eval/tuple-bind-fn->Evaluable (juxt inc dec) 1)}
        bizarro (pred/evaluable-predicate 'bizarro {:term-count 3 :required [0]})
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 3})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])])
               (rule/rule (r '[?x ?y ?z])
                          [(p '[1 ?x])
                           (bizarro '[?x ?y ?z])])]
        program (prog/program :edbs edbs,
                              :evaluators, evaluators,
                              :rules rules)
        q (r '[?x ?y ?z])]
    (is (= #{[2 3 1] [3 4 2] [4 5 3] [5 6 4] [6 7 5] [7 8 6]}
           (query program q)))))

(deftest unit:example-3-1-with-relation-binding-function
  (let [edbs {'q (edb/coll-of-tuples->EDB [[1 2] [1 3] [2 4] [3 5] [4 6] [5 7]])}
        evaluators {'bizarro (eval/relation-bind-fn->Evaluable (comp (partial map (juxt inc dec)) range) 1)}
        bizarro (pred/evaluable-predicate 'bizarro {:term-count 3 :required [0]})
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 3})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])])
               (rule/rule (r '[?x ?y ?z])
                          [(p '[1 ?x])
                           (bizarro '[?x ?y ?z])])]
        program (prog/program :edbs edbs,
                              :evaluators, evaluators,
                              :rules rules)
        q (r '[?x ?y ?z])]
    (is (= #{[2 1 -1] [2 2 0]
             [3 1 -1] [3 2 0] [3 3 1]
             [4 1 -1] [4 2 0] [4 3 1] [4 4 2]
             [5 1 -1] [5 2 0] [5 3 1] [5 4 2] [5 5 3]
             [6 1 -1] [6 2 0] [6 3 1] [6 4 2] [6 5 3] [6 6 4]
             [7 1 -1] [7 2 0] [7 3 1] [7 4 2] [7 5 3] [7 6 4] [7 7 5]}
           (query program q)))))

(deftest unit:example-3-1-with-satisfied-required-bindings
  (let [edbs {'q (edb/coll-of-tuples->EDB '[[a b] [a c] [b d] [c e] [d f] [e g]])}
        p (pred/rule-predicate 'p {:term-count 2 :required [0]})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 1})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])])
               (rule/rule (r '[?x])
                          [(p '[a ?x])])]
        program (prog/program :edbs edbs, :rules rules)
        q (r '[?m])]
    (is (= '#{[b] [c] [d] [e] [f] [g]}
           (query program q)))))

(deftest unit:example-3-1-with-unsatisfied-required-bindings
  (let [edbs {'q (edb/coll-of-tuples->EDB '[[a b] [a c] [b d] [c e] [d f] [e g]])}
        p (pred/rule-predicate 'p {:term-count 2, :required [1]})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 1})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])])
               (rule/rule (r '[?x])
                          [(p '[a ?x])])]
        program (prog/program :edbs edbs, :rules rules)
        q (r '[?m])]
    (is (thrown-with-msg? EvaException #"Insufficient bindings"
                          (query program q)))))

(deftest unit:example-3-1-but-cyclic-0
  (let [edbs {'q (edb/coll-of-tuples->EDB '[[a b] [a c] [b d] [c e] [d f] [e g] [g a]])}
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 1})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])])
               (rule/rule (r '[?x])
                          [(p '[a ?x])])]
        program (prog/program :edbs edbs, :rules rules)
        q (r '[?m])]
    (is (= '#{[a] [b] [c] [d] [e] [f] [g]}
           (query program q)))))

(deftest unit:example-3-1-but-cyclic-1
  (let [edbs {'q (edb/coll-of-tuples->EDB '[[a b] [a c] [b d] [c e] [d f] [e g] [g a]])
              's (edb/coll-of-tuples->EDB '[[b n] [g n]])}
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 1})
        f (pred/rule-predicate 'f {:term-count 1})
        s (pred/extensional-predicate 's {:term-count 2})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])])
               (rule/rule (f '[?m])
                          [(s '[?m n])])
               (rule/rule (r '[?x])
                          [(p '[a ?x])
                           (f '[?x])])]
        program (prog/program :edbs edbs, :rules rules)
        q (r '[?m])]
    (is (= '#{[g] [b]}
           (query program q)))))

(deftest unit:example-3-1-cyclic-with-another-connected-component
  (let [edbs {'q (edb/coll-of-tuples->EDB '[[a b] [a c] [b d] [c e] [d f] [e g] [g a] [x y] [y x]])}
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 1})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])])
               (rule/rule (r '[?x])
                          [(p '[a ?x])])]
        program (prog/program :edbs edbs, :rules rules)
        q (r '[?m])]
    (is (= '#{[a] [b] [c] [d] [e] [f] [g]}
           (query program q)))))

(deftest unit:addition
  (let [evaluators {'inc      (eval/scalar-bind-fn->Evaluable inc 1)
                    'dec      (eval/scalar-bind-fn->Evaluable dec 1)
                    'id       (eval/scalar-bind-fn->Evaluable identity 1)
                    'not-zero (eval/pred-fn->Evaluable (complement zero?))
                    'zero     (eval/pred-fn->Evaluable zero?)}
        pred-+ (pred/rule-predicate '+ {:term-count 3, :required [0 1]})
        not-zero (pred/evaluable-predicate 'not-zero {:term-count 1 :required [0]})
        pred-inc (pred/evaluable-predicate 'inc {:term-count 2 :required [0]})
        pred-dec (pred/evaluable-predicate 'dec {:term-count 2 :required [0]})
        zero (pred/evaluable-predicate 'zero {:term-count 1 :required [0]})
        id (pred/evaluable-predicate 'id {:term-count 2 :required [0]})
        r (pred/rule-predicate 'r {:term-count 3})
        rules [(rule/rule (pred-+ '[?x ?y ?z])
                          [(not-zero '[?y])
                           (pred-+ '[?a ?b ?z])
                           (pred-inc '[?x ?a])
                           (pred-dec '[?y ?b])])
               (rule/rule (pred-+ '[?x ?y ?z])
                          [(zero '[?y])
                           (id '[?x ?z])])
               (rule/rule (r '[?x ?y ?z])
                          [(pred-+ '[?x ?y ?z])])]
        program (prog/program :evaluators evaluators, :rules rules)
        goals (for [a (range 10) b (range 10)]
                [[a b] (r [a b '?z])])]
    (doseq [[[a b] g] goals]
      (is (= #{[a b (+ a b)]}
             (query program g))))))

(deftest unit:multiplication-sim
  (let [evaluators {'inc      (eval/scalar-bind-fn->Evaluable inc 1)
                    'dec      (eval/scalar-bind-fn->Evaluable dec 1)
                    'id       (eval/scalar-bind-fn->Evaluable identity 1)
                    'not-zero (eval/pred-fn->Evaluable (complement zero?))
                    'zero     (eval/pred-fn->Evaluable zero?)}
        *' (pred/rule-predicate '* {:term-count 3, :required [0 1]})
        +' (pred/rule-predicate '+ {:term-count 3, :required [0 1]})
        not-zero (pred/evaluable-predicate 'not-zero {:term-count 1 :required [0]})
        pred-inc (pred/evaluable-predicate 'inc {:term-count 2 :required [0]})
        pred-dec (pred/evaluable-predicate 'dec {:term-count 2 :required [0]})
        zero (pred/evaluable-predicate 'zero {:term-count 1 :required [0]})
        id (pred/evaluable-predicate 'id {:term-count 2 :required [0]})
        r (pred/rule-predicate 'r {:term-count 2})
        rules [(rule/rule (+' '[?x ?y ?z])
                          [(not-zero '[?y])
                           (+' '[?a ?b ?z])
                           (pred-inc '[?x ?a])
                           (pred-dec '[?y ?b])])
               (rule/rule (+' '[?x ?y ?z])
                          [(zero '[?y])
                           (id '[?x ?z])])
               (rule/rule (r '[?x ?z])
                          [(+' '[?x ?x ?a])
                           (+' '[?a ?x ?b])
                           (+' '[?b ?x ?c])
                           (+' '[?c ?x ?d])
                           (+' '[?d ?x ?e])
                           (+' '[?e ?x ?z])])]
        program (prog/program :evaluators evaluators, :rules rules)
        goal (r '[29 ?z])]
    (is (= #{[29 203]}
           (query program goal)))))

(deftest unit:multiplication
  (let [evaluators {'inc      (eval/scalar-bind-fn->Evaluable inc 1)
                    'dec      (eval/scalar-bind-fn->Evaluable dec 1)
                    'id       (eval/scalar-bind-fn->Evaluable identity 1)
                    'not-zero (eval/pred-fn->Evaluable (complement zero?))
                    'zero     (eval/pred-fn->Evaluable zero?)}
        *' (pred/rule-predicate '* {:term-count 3, :required [0 1]})
        +' (pred/rule-predicate '+ {:term-count 3, :required [0 1]})
        not-zero (pred/evaluable-predicate 'not-zero {:term-count 1 :required [0]})
        pred-inc (pred/evaluable-predicate 'inc {:term-count 2 :required [0]})
        pred-dec (pred/evaluable-predicate 'dec {:term-count 2 :required [0]})
        zero (pred/evaluable-predicate 'zero {:term-count 1 :required [0]})
        id (pred/evaluable-predicate 'id {:term-count 2 :required [0]})
        r (pred/rule-predicate 'r {:term-count 3})
        rules [(rule/rule (+' '[?x ?y ?z])
                          [(not-zero '[?y])
                           (+' '[?a ?b ?z])
                           (pred-inc '[?x ?a])
                           (pred-dec '[?y ?b])])
               (rule/rule (+' '[?x ?y ?z])
                          [(zero '[?y])
                           (id '[?x ?z])])
               (rule/rule (*' '[?x ?y ?z])
                          [(not-zero '[?y])
                           (pred-dec '[?y ?b])
                           (not-zero '[?b])
                           (*' '[?x ?b ?a])
                           (+' '[?a ?x ?z])])
               (rule/rule (*' '[?x ?y ?z])
                          [(pred-dec '[?y ?a])
                           (zero '[?a])
                           (id '[?x ?z])])
               (rule/rule (*' '[?x ?y ?z])
                          [(zero '[?y])
                           (id '[0 ?z])])
               (rule/rule (r '[?x ?y ?z])
                          [(*' '[?x ?y ?z])])]
        program (prog/program :evaluators evaluators, :rules rules)
        base-goal (r '[2 3 ?z])
        goals (for [a (repeatedly 2 #(inc (rand-int 25)))
                    b (repeatedly 2 #(rand-int 25))]
                [[a b] (r [a b '?z])])]
    (is (= #{[2 3 6]}
           (query program base-goal)))
    (doseq [[[a b] g] goals]
      (is (= #{[a b (* a b)]}
             (query program g))))))

(deftest unit:recursive-same-generation
  (let [edbs {'down (edb/coll-of-tuples->EDB '[[l f] [m f] [g b] [h c] [i d] [p k]])
              'flat (edb/coll-of-tuples->EDB '[[g f] [m n] [m o] [p m]])
              'up   (edb/coll-of-tuples->EDB '[[a e] [a f] [f m] [g n] [h n] [i o] [j o]])}
        rsg (pred/rule-predicate 'rsg {:term-count 2})
        flat (pred/extensional-predicate 'flat {:term-count 2})
        up (pred/extensional-predicate 'up {:term-count 2})
        down (pred/extensional-predicate 'down {:term-count 2})
        rules [(rule/rule (rsg '[?x ?y])
                          [(flat '[?x ?y])])
               (rule/rule (rsg '[?x ?y])
                          [(up '[?x ?x1])
                           (rsg '[?y1 ?x1])
                           (down '[?y1 ?y])])]
        program (prog/program :edbs edbs :rules rules)
        q1 (rsg '[a ?x])
        q2 (rsg '[?x d])]
    (is (= '#{[a b] [a c] [a d]}
           (query program q1)))
    (is (= '#{[a d]}
           (query program q2)))))

(deftest unit:simplest-negation-test
  (let [edbs {'q (edb/coll-of-tuples->EDB '[[a b] [a c] [a d]])
              'f (edb/coll-of-tuples->EDB '[[a c]])}
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        f (pred/extensional-predicate 'f {:term-count 2})
        s (pred/rule-predicate 's {:term-count 1})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])
                           (f '[?x ?y] :-)])
               (rule/rule (s '[?x])
                          [(p '[a ?x])])]
        program (prog/program :edbs edbs, :rules rules)
        q (s '[?x])]
    (is (= '#{[b] [d]}
           (query program q)))))

(deftest unit:negation-prototype
  (let [edbs {'q (edb/coll-of-tuples->EDB '[[a b] [a c] [b d] [c e] [d f] [e g]])}
        f (pred/rule-predicate 'f {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 1})
        rules [(rule/rule (f '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (r '[?x])
                          [(q '[?x ?y])
                           (f '[a ?y] :-)])]
        program (prog/program :edbs edbs, :rules rules)
        q (r '[?x])]
    (is (= '#{[b] [c] [d] [e]}
           (query program q)))))

(deftest unit:negation-prototype-2
  (let [edbs {'q (edb/coll-of-tuples->EDB '[[a b] [a c] [b d] [c e] [d f] [e g]])}
        f (pred/rule-predicate 'f {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 1})
        rules [(rule/rule (f '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (r '[?x])
                          [(q '[?x ?y])
                           (f '[a ?x] :-)])]
        program (prog/program :edbs edbs, :rules rules)
        q (r '[?x])]
    (is (= '#{[a] [e] [d]}
           (query program q)))))

(deftest unit:negated-rule-1
  (let [edbs {'edge (edb/coll-of-tuples->EDB '[[a b] [a c] [x y]])}
        find (pred/rule-predicate 'find {:term-count 1})
        edge (pred/extensional-predicate 'edge {:term-count 2})
        rules [(rule/rule (find '[?x])
                          [(edge '[?x ?y])
                           (edge '[?x c] :-)])]
        program (prog/program :edbs edbs, :rules rules)
        q (find '[?x])]
    (is (= '#{[x]}
           (query program q)))))

(deftest unit:negated-rule-2
  (let [edbs {'edge (edb/coll-of-tuples->EDB '[[a b] [a c] [x y] [x c]])}
        find (pred/rule-predicate 'find {:term-count 1})
        edge (pred/extensional-predicate 'edge {:term-count 2})
        rules [(rule/rule (find '[?x])
                          [(edge '[?x ?y])
                           (edge '[?x c] :-)])]
        program (prog/program :edbs edbs, :rules rules)
        q (find '[?x])]
    (is (= '#{}
           (query program q)))))

(deftest unit:negated-rule-3
  (let [edbs {'edge (edb/coll-of-tuples->EDB '[[a b] [a c] [x y] [x c]])}
        find (pred/rule-predicate 'find {:term-count 1})
        edge (pred/extensional-predicate 'edge {:term-count 2})
        rules [(rule/rule (find '[?x])
                          [(edge '[?x ?y])
                           (edge '[?x q] :-)])]
        program (prog/program :edbs edbs, :rules rules)
        q (find '[?x])]
    (is (= '#{[a] [x]}
           (query program q)))))

(deftest unit:recursive-negation
  (let [edbs {'edge  (edb/coll-of-tuples->EDB '[[a b] [a d] [b c] [b e] [c f] [d e]
                                                [d g] [e h] [e f] [f i] [g h] [h i]])
              'black (edb/coll-of-tuples->EDB '[[e]])}
        edge (pred/extensional-predicate 'edge {:term-count 2})
        black (pred/extensional-predicate 'black {:term-count 1})
        tainted (pred/rule-predicate 'tainted {:term-count 1})
        rules [(rule/rule (tainted '[?x])
                          [(black '[?x])])
               (rule/rule (tainted '[?x])
                          [(edge '[?y ?x])
                           (tainted '[?y] :-)])]
        program (prog/program :edbs edbs, :rules rules)
        goal (tainted '[?x])]
    (is (= '#{[e] [b] [d] [f] [h]}
           (query program goal)))))

(deftest unit:negate-ancestor-subtree
  (let [edbs {'q (edb/coll-of-tuples->EDB '[[a b] [a c] [b d] [c e] [d f] [e g]])}
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 1})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])])
               (rule/rule (r '[?x])
                          [(p '[a ?x])
                           (p '[c ?x] :-)])]
        program (prog/program :edbs edbs, :rules rules)
        q (r '[?x])]
    (is (= '#{[b] [c] [d] [f]}
           (query program q)))))

(deftest unit:negate-ancestor-cyclic
  (let [edbs {'q (edb/coll-of-tuples->EDB '[[a b] [a c] [b d] [c e] [d f] [e g] [g a]])}
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 1})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])])
               (rule/rule (r '[?x])
                          [(p '[a ?x])
                           (p '[c ?x] :-)])]
        program (prog/program :edbs edbs, :rules rules)
        q (r '[?x])]
    (is (= '#{}
           (query program q)))))

(deftest unit:deeper-negation
  (let [edbs {'p (edb/coll-of-tuples->EDB '[[a b] [b c] [c d] [d e] [e f] [f g] [g h] [h i] [i j] [g a]])
              'q (edb/coll-of-tuples->EDB '[[a f] [a e] [f b] [e c] [b d] [d j]])}
        p-anc (pred/rule-predicate 'p-anc {:term-count 2})
        p (pred/extensional-predicate 'p {:term-count 2})
        q-anc (pred/rule-predicate 'q-anc {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 2})
        rules [(rule/rule (p-anc '[?x ?y])
                          [(p '[?x ?y])])
               (rule/rule (p-anc '[?x ?y])
                          [(p '[?x ?z])
                           (p-anc '[?z ?y])])
               (rule/rule (q-anc '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (q-anc '[?x ?y])
                          [(q '[?x ?z])
                           (q-anc '[?z ?y])])
               (rule/rule (r '[?x ?y])
                          [(p-anc '[?x ?y])
                           (p-anc '[?y ?x] :-)
                           (q-anc '[?x ?y])])]
        program (prog/program :edbs edbs, :rules rules)
        q (r '[a ?x])]
    (is (= '#{[a j]}
           (query program q)))))

(deftest unit:layered-negation
  (let [edbs {'r (edb/coll-of-tuples->EDB '[[a] [b] [c]])
              'g (edb/coll-of-tuples->EDB '[[d] [e] [f]])
              'b (edb/coll-of-tuples->EDB '[[g] [h] [i]])
              'c (edb/coll-of-tuples->EDB '[[g h] [i h] [i g] [g e]
                                            [d e] [e f] [e b] [f e] [f a] [f c]
                                            [c b] [b c] [a b] [b d] [b e] [b i]])}
        jar (pred/rule-predicate 'jar {:term-count 1})
        jam (pred/rule-predicate 'jam {:term-count 1})
        b (pred/extensional-predicate 'b {:term-count 1})
        c (pred/extensional-predicate 'c {:term-count 2})
        bottle (pred/rule-predicate 'bottle {:term-count 1})
        g (pred/extensional-predicate 'g {:term-count 1})
        cadence (pred/rule-predicate 'cadence {:term-count 1})
        r (pred/extensional-predicate 'r {:term-count 1})
        symphony (pred/rule-predicate 'symphony {:term-count 1})
        flask (pred/rule-predicate 'flask {:term-count 1})
        milk (pred/rule-predicate 'milk {:term-count 1})
        cadential (pred/rule-predicate 'cadential {:term-count 1})
        howl (pred/rule-predicate 'howl {:term-count 1})
        star (pred/rule-predicate 'star {:term-count 1})
        moon (pred/rule-predicate 'moon {:term-count 1})
        comet (pred/rule-predicate 'comet {:term-count 1})
        rules [(rule/rule (jar '[?x])
                          [(b '[?x])
                           (c '[?x ?y])
                           (b '[?y])])
               (rule/rule (jam '[?x])
                          [(c '[?y ?x])
                           (jar '[?y])])
               (rule/rule (bottle '[?x])
                          [(g '[?x])
                           (c '[?x ?y])
                           (g '[?y])])
               (rule/rule (cadence '[?x])
                          [(bottle '[?x])
                           (jam '[?x] :-)])
               (rule/rule (milk '[?x])
                          [(c '[?y ?x])
                           (bottle '[?y])])
               (rule/rule (flask '[?x])
                          [(r '[?x])
                           (c '[?x ?y])
                           (r '[?y])])
               (rule/rule (symphony '[?x])
                          [(flask '[?x])
                           (milk '[?x] :-)])
               (rule/rule (cadential '[?x])
                          [(c '[?y ?x])
                           (cadence '[?y])])
               (rule/rule (howl '[?x])
                          [(flask '[?x])
                           (cadential '[?x] :-)])
               (rule/rule (moon '[?x])
                          [(c '[?y ?x])
                           (howl '[?y])])
               (rule/rule (star '[?x])
                          [(c '[?x ?y])
                           (moon '[?x] :-)])
               (rule/rule (comet '[?x])
                          [(c '[?y ?x])
                           (moon '[?x] :-)
                           (star '[?x] :-)])]
        program (prog/program :edbs edbs, :rules rules)
        f (fn [s] (let [q (s '[?x])]
                    (query program q)))]
    (is (= (f jar) '#{[g] [i]}))
    (is (= (f bottle) '#{[d] [e] [f]}))
    (is (= (f flask) '#{[a] [b] [c]}))
    (is (= (f jam) '#{[e] [h] [g]}))
    (is (= (f cadence) '#{[d] [f]}))
    (is (= (f milk) '#{[e] [f] [a] [c] [b]}))
    (is (= (f symphony) '#{}))
    (is (= (f cadential) '#{[e] [a] [c]}))
    (is (= (f howl) '#{[b]}))
    (is (= (f moon) '#{[d] [i] [c] [e]}))
    (is (= (f star) '#{[b] [g] [a] [f]}))
    (is (= (f comet) '#{[h]}))))

(deftest unit:double-negation
  (let [edbs {'q (edb/coll-of-tuples->EDB '[[a] [b]])
              'p (edb/coll-of-tuples->EDB '[[a]])}
        prule (pred/rule-predicate 'prule {:term-count 1})
        p (pred/extensional-predicate 'p {:term-count 1})
        neg (pred/rule-predicate 'neg {:term-count 1})
        q (pred/extensional-predicate 'q {:term-count 1})
        negneg (pred/rule-predicate 'negneg {:term-count 1})
        rules [(rule/rule (prule '[?x])
                          [(p '[?x])])
               (rule/rule (neg '[?x])
                          [(prule '[?x] :-)
                           (q '[?x])])
               (rule/rule (negneg '[?x])
                          [(neg '[?x] :-)
                           (q '[?x])])]
        program (prog/program :edbs edbs, :rules rules)
        q (negneg '[?x])]
    (is (= '#{[a]}
           (query program q)))))

(deftest unit:negate-ancestor-subtrees
  (let [edbs {'q (edb/coll-of-tuples->EDB '[[a b] [a c] [b d] [c e] [d f] [e g]])}
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        r (pred/rule-predicate 'r {:term-count 1})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(p '[?x ?z])
                           (q '[?z ?y])])
               (rule/rule (r '[?x])
                          [(p '[a ?x])
                           (p '[c ?x] :-)
                           (p '[b ?x] :-)])]
        program (prog/program :edbs edbs, :rules rules)
        q (r '[?x])]
    (is (= '#{[b] [c]}
           (query program q)))))

(deftest unit:circular-ancestry
  (let [edbs {'q (edb/coll-of-tuples->EDB '[[a b] [b c] [c a]])}
        p (pred/rule-predicate 'p {:term-count 2})
        q (pred/extensional-predicate 'q {:term-count 2})
        rules [(rule/rule (p '[?x ?y])
                          [(q '[?x ?y])])
               (rule/rule (p '[?x ?y])
                          [(q '[?x ?z])
                           (p '[?z ?y])])]
        program (prog/program :edbs edbs, :rules rules)
        q (p '[?x ?x])]
    (is (= '#{[a a] [b b] [c c]}
           (query program q)))))

(deftest unit:time-travellers-spouse-filter
  (let [edbs {'grandfathered (edb/coll-of-tuples->EDB '[[Frank Fred]
                                                        [Frank Cicero]
                                                        [Frank George]
                                                        [Fred Douglas]
                                                        [Douglas Frank]
                                                        [George Michael]
                                                        [Michael Frank]
                                                        [Dante George]
                                                        [Dante QueeQueg]
                                                        [QueeQueg Fred]
                                                        [QueeQueg Robert]
                                                        [Stephen Douglas]
                                                        [Robert Stephen]
                                                        [Robert Ishmael]
                                                        [Ishmael Cicero]
                                                        [Robert Cicero]])}
        ancestor (pred/rule-predicate 'ancestor {:term-count 2})
        ancestor-1 (pred/rule-predicate 'ancestor-1 {:term-count 1})
        grandfathered (pred/extensional-predicate 'grandfathered {:term-count 2})
        well-adjusted (pred/rule-predicate 'well-adjusted {:term-count 1})
        ineligible (pred/rule-predicate 'ineligible {:term-count 1})
        find (pred/rule-predicate 'find {:term-count 1})
        rules [(rule/rule (ancestor '[?x ?y])
                          [(grandfathered '[?x ?y])])
               (rule/rule (ancestor-1 '[?x])
                          [(grandfathered '[?x ?x])])
               (rule/rule (ancestor '[?x ?y])
                          [(grandfathered '[?x ?z])
                           (ancestor '[?z ?y])])
               (rule/rule (ancestor-1 '[?x])
                          [(grandfathered '[?x ?z])
                           (ancestor '[?z ?x])])
               (rule/rule (well-adjusted '[?x])
                          [(ancestor-1 '[?x])])
               (rule/rule (ineligible '[?x])
                          [(well-adjusted '[?x])])
               (rule/rule (find '[?x])
                          [(grandfathered '[?x ?y])
                           (ineligible '[?x] :-)])
               (rule/rule (find '[?x])
                          [(grandfathered '[?y ?x])
                           (ineligible '[?x] :-)])]
        program (prog/program :edbs edbs, :rules rules)
        q (find '[?x])
        q1 (well-adjusted '[?x])]
    (is (= '#{[Ishmael] [Stephen] [Robert] [QueeQueg] [Dante] [Cicero]}
           (query program q)))
    (is (= '#{[George] [Frank] [Fred] [Douglas] [Michael]}
           (query program q1)))))
