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

(ns eva.query.dialect.pull.parser
  (:require [eva.query.dialect.pull.error :as perr]
            [eva.query.dialect.util :refer [ASTNode expression]]
            [eva.query.dialect.pull.ast :as ast])
  (:import (clojure.lang ExceptionInfo)))

;; Pattern Grammar
;;
;; pattern            = [attr-spec+]
;; attr-spec          = attr-name | wildcard | map-spec | attr-expr
;; attr-name          = an edn keyword that names an attr
;; wildcard           = "*" or '*'
;; map-spec           = { ((attr-name | limit-expr) (pattern | recursion-limit))+ }
;; attr-expr          = limit-expr | default-expr
;; limit-expr         = [("limit" | 'limit') attr-name (positive-number | nil)]
;; default-expr       = [("default" | 'default') attr-name any-value]
;; recursion-limit    = positive-number | '...'

;; Predicates that test for elements of the pattern grammar
(def positive-number? (every-pred number? pos?))
(declare pattern? attr-spec? attr-name? wildcard? map-spec? attr-expr? limit-expr? recursion-limit? default-expr?)
(def attr-spec? (some-fn #'attr-name? #'wildcard? #'map-spec? #'attr-expr?))
(def attr-name? keyword?)
(def pattern? (every-pred sequential?
                          not-empty
                          (partial every? attr-spec?)))
(def wildcard? (partial = '*))
(def map-spec? (every-pred map? not-empty
                           (comp (partial every? (some-fn #'attr-name? #'limit-expr?)) keys)
                           (comp (partial every? (some-fn #'pattern? #'recursion-limit?)) vals)))
(def attr-expr? (some-fn #'limit-expr? #'default-expr?))
(def limit-expr? (every-pred sequential? not-empty #(= 3 (count %))
                             #(= 'limit (first %))
                             #(attr-name? (second %))
                             (comp (some-fn positive-number? nil?) last)))
(def default-expr? (every-pred sequential? not-empty #(= 3 (count %))
                               #(= 'default (first %))
                               #(attr-name? (second %))
                               (comp some? last)))
(def recursion-limit? (some-fn positive-number? #(= '... %)))

;; ast generation functions

(declare expr->ast)

(defn- assert-expr [expr-pred type-key expr]
  (if-not (expr-pred expr)
    (perr/raise-syntax-error (format "invalid %s, %s" (name type-key) expr)
                             {:type (keyword "pull-datalog.invalid" (name type-key))
                              :expression expr})
    expr))

(defn choice [fns]
  (letfn [(suppress-ast-err [f] (try (f) (catch RuntimeException e nil)))]
    (->> fns
         (map suppress-ast-err)
         (remove nil?)
         (first))))

(defn expr-choice [expr fns]
  (->> fns
       (map #(partial % expr))
       (choice)))

(defn wildcard->ast [expr]
  (assert-expr wildcard? :wildcard expr)
  ast/wildcard-value)

(defn attr-name->ast [expr]
  (assert-expr attr-name? :attr-name expr)
  (ast/->Attribute expr))

(defn limit-expr->ast [expr]
  (assert-expr limit-expr? :limit-expr expr)
  (let [[_ attr-name n] expr]
    (ast/map->LimitExpr {:attribute (attr-name->ast attr-name)
                         :limit     n})))

(defn default-expr->ast [expr]
  (assert-expr default-expr? :default-expr expr)
  (let [[_ attr-name v] expr]
    (ast/map->DefaultExpr {:attribute (attr-name->ast attr-name)
                           :default   v})))

(defn recursion-limit->ast [expr]
  (assert-expr recursion-limit? :recursion-limit expr)
  (ast/->RecursionLimit (let [n expr] (when (number? n) (long n)))))

(declare pattern->ast)

(defn map-spec->ast [expr]
  (assert-expr map-spec? :map-spec expr)
  (ast/->MapSpec (map (fn [[k v]]
                        (ast/->MapSpecEntry (expr-choice k [attr-name->ast limit-expr->ast])
                                            (expr-choice v [pattern->ast recursion-limit->ast])))
                  expr)))

(defn attr-expr->ast [expr]
  (assert-expr attr-expr? :attr-expr expr)
  (cond (limit-expr? expr) (limit-expr->ast expr)
        (default-expr? expr) (default-expr->ast expr)))

(defn attr-spec->ast [expr]
  (assert-expr attr-spec? :attr-spec expr)
  (cond (attr-name? expr) (attr-name->ast expr)
        (wildcard? expr) (wildcard->ast expr)
        (map-spec? expr) (map-spec->ast expr)
        (attr-expr? expr) (attr-expr->ast expr)))

(defn pattern->ast [expr]
  (assert-expr pattern? :pattern expr)
  (ast/->Pattern (into [] (map attr-spec->ast) expr)))
