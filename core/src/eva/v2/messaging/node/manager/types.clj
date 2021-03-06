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

(ns eva.v2.messaging.node.manager.types
  (:require [clojure.spec.alpha :as s]))

;;;;;;;;;;
;; SPEC ;;
;;;;;;;;;;

;; TODO: multispec
(s/def ::config map?)

;;;;;;;;;;;;;;
;; END SPEC ;;
;;;;;;;;;;;;;;

(defmulti messenger-node-discriminator
  "Returns a unique 'identity' for a messenger node for use
   in a resource manager."
  (fn [_ config] (:messenger-node-config/type config)))

(defmulti messenger-node-constructor
  "Constructs and returns a SharedResource messenger node that satisfies
   the various messaging protocols"
  (fn [_ config] (:messenger-node-config/type config)))
