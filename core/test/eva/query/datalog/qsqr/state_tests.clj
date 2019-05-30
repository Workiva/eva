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

(ns eva.query.datalog.qsqr.state-tests
  (:require [eva.query.datalog.qsqr.state :refer :all]
            [eva.query.datalog.predicate :as pred]
            [eva.query.datalog.protocols :as p]
            [eva.query.datalog.qsqr.protocols :as qp]
            [clojure.test :refer :all]))

;; protocol function add-query is tested throughout the following unit tests.

(deftest unit:query.datalog:state:update-derived
  (let [sym 'cantle
        predicate (-> ((pred/rule-predicate sym {:term-count 3}) '[?x ?y ?z])
                      (predicate->generalized '#{{?x 1 ?y 2 ?z 3} {?x 4 ?y 5 ?z 6}}))
        state (qp/add-query init-state predicate)
        results-all #{[1 2 3] [4 5 6]}
        results-first #{[1 2 3]}
        results-second #{[4 5 6]}
        results-none #{}]
    (is (= false (contains? (qp/derived state) sym)) "What?")
    (is (= true
           (contains? (qp/derived (update-derived state predicate results-all)) sym)
           (contains? (qp/derived (update-derived state predicate results-first)) sym)
           (contains? (qp/derived (update-derived state predicate results-second)) sym)
           (contains? (qp/derived (update-derived state predicate results-none)) sym))
        "I'm not sure where update-derived is putting things...")
    (letfn [(get-derived [results] (-> (update-derived state predicate results)
                                       (qp/derived)
                                       (get sym)))]
      (are [x] (= x (get-derived x)) ;; put it into an empty state, get it back out.
        results-all
        results-first
        results-second
        results-none))
    (is (= (-> state
               (update-derived predicate results-first)
               (update-derived predicate results-none)
               (update-derived predicate results-second)
               (qp/derived))
           (-> state
               (update-derived predicate results-second)
               (update-derived predicate results-first)
               (update-derived predicate results-none)
               (qp/derived))
           (-> state
               (update-derived predicate results-all)
               (qp/derived))
           (-> state
               (update-derived predicate results-all)
               (update-derived predicate results-second)
               (update-derived predicate results-none)
               (qp/derived)))
        "Ordering is apparently mattering to update-derived.")))

(deftest unit:query.datalog:state:remove-unifiable
  (let [sym 'verb
        predicate ((pred/extensional-predicate sym {:term-count 4}) '[?aspect ?mood ?voice Plural] :-)
        prior-results '#{[Simple Indicative Middle Plural]
                         [Simple Indicative Middle Singular]
                         [Completed Subjunctive Active Singular]
                         [Completed Subjunctive Passive Plural]}
        bindings '#{{?aspect Simple ?voice Middle}
                    {?aspect Completed ?voice Active}}
        state (-> init-state
                  (update-derived predicate prior-results)
                  (assoc :bindings bindings))
        query (predicate->generalized predicate (qp/bindings state))]
    (is (= bindings (qp/bindings state)) "Test setup failed fundamentally. Other tests should catch this.")
    (is (= [0 2 3] (p/decoration query)) "Test setup failed fundamentally. Other tests should catch this.")
    (is (= '#{[Simple Middle Plural]
              [Completed Active Plural]}
           (p/bound-terms query))
        "Test setup failed fundamentally. Other tests should catch this.")
    (is (clojure.set/superset? (remove-unifiable (qp/bindings state)
                                                 query
                                                 (get (qp/derived state) sym))
                               '#{{?aspect Completed ?voice Active}})
        "The result of remove-unifiable does not contain the bindings map it should contain.")
    (is (clojure.set/superset? '#{{?aspect Completed ?voice Active}}
                               (remove-unifiable (qp/bindings state)
                                                 query
                                                 (get (qp/derived state) sym)))
        "The result of remove-unifiable contains superfluous bindings.")))

(deftest unit:query.datalog:state:add-unifiable
  (let [sym 'verb
        predicate ((pred/extensional-predicate sym {:term-count 4}) '[?aspect ?mood ?voice Plural])
        prior-results '#{[Simple Indicative Middle Plural]
                         [Simple Indicative Middle Singular]
                         [Completed Subjunctive Active Singular]
                         [Completed Subjunctive Passive Plural]}
        bindings '#{{?aspect Simple ?voice Middle}
                    {?aspect Completed ?voice Active}}
        state (-> init-state
                  (update-derived predicate prior-results)
                  (assoc :bindings bindings))
        query (predicate->generalized predicate (qp/bindings state))]
    (is (= bindings (qp/bindings state)) "Test setup failed fundamentally. Other tests should catch this.")
    (is (= [0 2 3] (p/decoration query)) "Test setup failed fundamentally. Other tests should catch this.")
    (is (= '#{[Simple Middle Plural]
              [Completed Active Plural]}
           (p/bound-terms query))
        "Test setup failed fundamentally. Other tests should catch this.")
    (is (clojure.set/superset? (add-unifiable (qp/bindings state)
                                              query
                                              (get (qp/derived state) sym))
                               '#{{?aspect Simple ?voice Middle ?mood Indicative}})
        "The result of add-unifiable does not contain the bindings map it should contain.")
    (is (clojure.set/superset? '#{{?aspect Simple ?voice Middle ?mood Indicative}}
                               (add-unifiable (qp/bindings state)
                                              query
                                              (get (qp/derived state) sym)))
        "The result of add-unifiable contains superfluous bindings.")))

(deftest unit:query.datalog:state:update-bindings
  ;; Common definitions to all tests in this unit
  (let [sym 'arrow
        derived '#{[simple plastic wood blunt]
                   [simple feather wood blunt]
                   [simple plastic ceramic broadhead]
                   [cross feather ceramic broadhead]}
        dummy-predicate ((pred/rule-predicate sym {:term-count 4}) '[?nock ?fletching ?shaft ?head])
        blank-state (update-derived init-state dummy-predicate derived)
        state-fn (fn [bindings] (assoc blank-state :bindings bindings))]
    ;; TEST 1: positive
    (let [predicate ((pred/extensional-predicate sym {:term-count 4}) '[?nock ?fletching ?shaft ?head])
          bindings '#{{?nock simple, ?fletching plastic, ?head blunt}
                      {?nock simple, ?fletching plastic, ?head broadhead}
                      {?nock simple, ?fletching feather, ?head blunt}
                      {?nock simple, ?fletching feather, ?head broadhead}}
          state (state-fn bindings)
          query (predicate->generalized predicate (qp/bindings state))]
      (is (= '#{[simple plastic blunt]
                [simple plastic broadhead]
                [simple feather blunt]
                [simple feather broadhead]}
             (p/bound-terms query))
          "Test setup failed fundamentally. Other tests should also catch this.")
      (is (clojure.set/superset? (qp/bindings (conjunct-bindings state query))
                                 '#{{?nock simple, ?fletching plastic, ?shaft wood, ?head blunt}
                                    {?nock simple, ?fletching plastic, ?shaft ceramic, ?head broadhead}
                                    {?nock simple, ?fletching feather, ?shaft wood, ?head blunt}})
          "Update-bindings is leaving out some bindings.")
      (is (clojure.set/superset? '#{{?nock simple, ?fletching plastic, ?shaft wood, ?head blunt}
                                    {?nock simple, ?fletching plastic, ?shaft ceramic, ?head broadhead}
                                    {?nock simple, ?fletching feather, ?shaft wood, ?head blunt}}
                                 (qp/bindings (conjunct-bindings state query)))
          "conjunct-bindings is generating superfluous bindings."))
    ;; TEST 2: positive
    (let [predicate ((pred/extensional-predicate sym {:term-count 4}) '[?nock plastic ?shaft ?head])
          bindings '#{{?nock simple, ?shaft wood}
                      {?nock simple, ?shaft ceramic}
                      {?nock cross, ?shaft wood}
                      {?nock cross, ?shaft ceramic}}
          state (state-fn bindings)
          query (predicate->generalized predicate (qp/bindings state))]
      (is (= '#{[simple plastic wood]
                [simple plastic ceramic]
                [cross plastic wood]
                [cross plastic ceramic]}
             (p/bound-terms query))
          "Test setup failed fundamentally. Other tests should also catch this.")
      (is (clojure.set/superset? (qp/bindings (conjunct-bindings state query))
                                 '#{{?nock simple, ?shaft wood, ?head blunt}
                                    {?nock simple, ?shaft ceramic, ?head broadhead}})
          "conjunct-bindings is leaving out some bindings.")
      (is (clojure.set/superset? '#{{?nock simple, ?shaft wood, ?head blunt}
                                    {?nock simple, ?shaft ceramic, ?head broadhead}}
                                 (qp/bindings (conjunct-bindings state query)))
          "conjunct-bindings is generating superfluous bindings."))
    ;; TEST 3: negative
    (let [predicate ((pred/extensional-predicate sym {:term-count 4}) '[?nock plastic ?shaft ?head] :-)
          bindings '#{{?shaft ceramic, ?head broadhead}
                      {?shaft wood, ?head broadhead}
                      {?shaft ceramic, ?head blunt}
                      {?shaft wood, ?head blunt}}
          state (state-fn bindings)
          query (predicate->generalized predicate (qp/bindings state))]
      (is (= '#{[plastic ceramic broadhead]
                [plastic wood broadhead]
                [plastic ceramic blunt]
                [plastic wood blunt]}
             (p/bound-terms query))
          "Test setup failed fundamentally. Other tests should also catch this.")
      (is (clojure.set/superset? (qp/bindings (conjunct-bindings state query))
                                 '#{{?shaft wood, ?head broadhead}
                                    {?shaft ceramic, ?head blunt}})
          "conjunct-bindings is leaving out some bindings.")
      (is (clojure.set/superset? '#{{?shaft wood, ?head broadhead}
                                    {?shaft ceramic, ?head blunt}}
                                 (qp/bindings (conjunct-bindings state query)))
          "conjunct-bindings is generating superfluous bindings."))
    ;; TEST 4: negative
    (let [predicate ((pred/extensional-predicate sym {:term-count 4}) '[?nock ?fletching ?shaft broadhead] :-)
          bindings '#{{?nock simple, ?fletching plastic, ?shaft wood}
                      {?nock simple, ?fletching feather, ?shaft wood}
                      {?nock simple, ?fletching plastic, ?shaft ceramic}
                      {?nock cross, ?fletching feather, ?shaft ceramic}
                      {?nock funky, ?fletching stringy, ?shaft paper}}
          state (state-fn bindings)
          query (predicate->generalized predicate (qp/bindings state))]
      (is (= '#{[simple plastic wood broadhead]
                [simple feather wood broadhead]
                [simple plastic ceramic broadhead]
                [cross feather ceramic broadhead]
                [funky stringy paper broadhead]}
             (p/bound-terms query))
          "Test setup failed fundamentally. Other tests should also catch this.")
      (is (clojure.set/superset? (qp/bindings (conjunct-bindings state query))
                                 '#{{?nock simple, ?fletching plastic, ?shaft wood}
                                    {?nock simple, ?fletching feather, ?shaft wood}
                                    {?nock funky, ?fletching stringy, ?shaft paper}})
          "conjunct-bindings is leaving out some bindings.")
      (is (clojure.set/superset? '#{{?nock simple, ?fletching plastic, ?shaft wood}
                                    {?nock simple, ?fletching feather, ?shaft wood}
                                    {?nock funky, ?fletching stringy, ?shaft paper}}
                                 (qp/bindings (conjunct-bindings state query)))
          "conjunct-bindings is generating superfluous bindings."))))

(deftest unit:query.datalog:state:predicate->generalized
  (let [predicate-1 ((pred/rule-predicate 'foo {:term-count 3}) '[?x ?y ?z])
        predicate-2 ((pred/evaluable-predicate 'foo {:term-count 3}) '[?x ?y 1])
        bindings-1 '#{{?a 5 ?x 3 ?y 2 ?z 1} ;; binds all three; has superfluous binding (or two)
                      {?a 1 ?x 2 ?y 3 ?z 5}}
        bindings-2 '#{{?x 1 ?y 2} ;; binds first two
                      {?x 2 ?y 1}
                      {?x 2 ?y 2}
                      {?x 1 ?y 1}}
        bindings-3 '#{{?x 1 ?z 1}} ;; binds first, or first and third
        gen-1-1 (predicate->generalized predicate-1 bindings-1)
        gen-1-2 (predicate->generalized predicate-1 bindings-2)
        gen-1-3 (predicate->generalized predicate-1 bindings-3)
        gen-2-1 (predicate->generalized predicate-2 bindings-1)
        gen-2-2 (predicate->generalized predicate-2 bindings-2)
        gen-2-3 (predicate->generalized predicate-2 bindings-3)]
    (is (= (p/decoration gen-1-1)
           (p/decoration gen-2-1)
           (p/decoration gen-2-2)
           [0 1 2])
        "Error in generated decoration for generalized predicate.")
    (is (= (p/decoration gen-1-2) [0 1])
        "Error in generated decoration for generalized predicate.")
    (is (= (p/decoration gen-1-3)
           (p/decoration gen-2-3)
           [0 2])
        "Error in generated decoration for generalized predicate.")
    (is (= (p/bound-terms gen-1-1) #{[3 2 1] [2 3 5]})
        "Error in generated bound-terms for generalized predicate.")
    (is (= (p/bound-terms gen-1-2) #{[1 1] [1 2] [2 1] [2 2]})
        "Error in generated bound-terms for generalized predicate.")
    (is (= (p/bound-terms gen-1-3)
           (p/bound-terms gen-2-3)
           #{[1 1]})
        "Error in generated bound-terms for generalized predicate.")
    (is (= (p/bound-terms gen-2-1) #{[3 2 1] [2 3 1]})
        "Error in generated bound-terms for generalized predicate.")
    (is (= (p/bound-terms gen-2-2) #{[1 1 1] [1 2 1] [2 1 1] [2 2 1]})
        "Error in generated bound-terms for generalized predicate.")))

(deftest unit:query.datalog:state:novel-generalization
  (let [proto-liszt-1 (-> ((pred/extensional-predicate 'liszt {:term-count 5}) '[?a ?b ?c ?d ?e])
                          (predicate->generalized '#{{?a 0 ?b 0} {?a 1 ?b 1}}))
        liszt-1 (-> ((pred/extensional-predicate 'liszt {:term-count 5}) '[?a ?b ?c ?d ?e])
                    (predicate->generalized '#{{?a 0 ?b 0} {?a 1 ?b 1} {?a 5 ?b 5}}))
        chopin-1 (-> ((pred/extensional-predicate 'chopin {:term-count 5}) '[?a ?b ?c ?d ?e])
                     (predicate->generalized '#{{?a 0 ?b 0} {?a 1 ?b 1} {?a 5 ?b 5}}))
        proto-liszt-2 (-> ((pred/extensional-predicate 'liszt {:term-count 5}) '[?a ?b ?c ?d ?e])
                          (predicate->generalized '#{{?a 6 ?b 2 ?e 19} {?a 5 ?b 5 ?e 2}}))
        liszt-2 (-> ((pred/extensional-predicate 'liszt {:term-count 5}) '[?a ?b ?c ?d ?e])
                    (predicate->generalized '#{{?a 0 ?b 0 ?e 2} {?a 1 ?b 1 ?e 2} {?a 5 ?b 5 ?e 2}}))
        empty-state init-state
        state-1 (qp/add-query init-state liszt-1)
        state-2 (qp/add-query init-state liszt-2)
        state-both (reduce qp/add-query init-state [liszt-1 liszt-2])
        state-all (reduce qp/add-query init-state [chopin-1 liszt-1 liszt-2])
        proto-state-1 (qp/add-query init-state proto-liszt-1)
        proto-state-2 (qp/add-query init-state proto-liszt-2)
        proto-state-both (reduce qp/add-query init-state [proto-liszt-1 proto-liszt-2])]
    (is (= nil
           (novel-generalization state-1 liszt-1)
           (novel-generalization state-2 liszt-2)
           (novel-generalization state-both liszt-1)
           (novel-generalization state-both liszt-2)
           (novel-generalization state-all chopin-1)
           (novel-generalization state-all liszt-1)
           (novel-generalization state-all liszt-2))
        "novel-generalization is not returning nil for repeated queries.")
    (is (= #{[5 5]}
           (p/bound-terms (novel-generalization proto-state-1 liszt-1))
           (p/bound-terms (novel-generalization proto-state-both liszt-1)))
        "novel-generalization is not filtering previously seen queries correctly.")
    (is (= #{[0 0 2] [1 1 2]}
           (p/bound-terms (novel-generalization proto-state-2 liszt-2))
           (p/bound-terms (novel-generalization proto-state-both liszt-2)))
        "novel-generalization is not filtering previously seen queries correctly.")
    (are [x y] (= x (novel-generalization y x)) ;; returns queries unchanged.
      liszt-1 empty-state
      liszt-1 state-2
      liszt-2 empty-state
      liszt-2 state-1
      chopin-1 empty-state
      chopin-1 state-1
      chopin-1 state-2
      chopin-1 state-both
      chopin-1 proto-state-both)))
