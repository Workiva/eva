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

(ns eva.query.dialect.util)

(defprotocol ASTNode
  (expression [ast] "returns the original expression that produced the ASTNode")
  (children [ast] "returns child ASTNodes"))

(def default-ast-node-impl {:expression identity
                            :children (constantly nil)})
(def default-ast-types [String
                        Boolean
                        Number
                        clojure.lang.Keyword
                        java.util.Date
                        java.util.UUID
                        java.net.URI
                        (type (byte-array 0))
                        eva.ByteString])

(doseq [t default-ast-types]
  (extend t ASTNode default-ast-node-impl))
