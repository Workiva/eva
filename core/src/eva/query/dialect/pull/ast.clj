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

(ns eva.query.dialect.pull.ast
  (:require [eva.query.dialect.util :refer [ASTNode expression]]))

(defrecord Wildcard []
  ASTNode
  (expression [_] '*)
  (children [_] nil))

(def wildcard-value (->Wildcard))

(defrecord Attribute [name]
  ASTNode
  (expression [_] name)
  (children [_] nil))

(defrecord LimitExpr [attribute limit]
  ASTNode
  (expression [_] (list 'limit (expression attribute) limit))
  (children [_] [attribute]))

(defrecord DefaultExpr [attribute default]
  ASTNode
  (expression [_] (list 'default (expression attribute) default))
  (children [_] [attribute]))

(defrecord RecursionLimit [value]
  ASTNode
  (expression [_] (if (some? value) value '...))
  (children [_] nil))

(defrecord MapSpec [entries]
  ASTNode
  (expression [_] (into {} (map expression) entries))
  (children [_] (seq entries)))

(defrecord MapSpecEntry [attribute value]
  ASTNode
  (expression [_] [(expression attribute) (expression value)])
  (children [_] [attribute value]))

(defrecord Pattern [args]
  ASTNode
  (expression [_] (mapv expression args))
  (children [_] (seq args)))

