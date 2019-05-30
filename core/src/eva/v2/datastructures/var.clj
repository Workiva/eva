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

(ns eva.v2.datastructures.var
  (:require [eva.v2.datastructures.atom :refer [->PersistedAtom]]
            [eva.v2.storage.value-store :as vs]
            [eva.datastructures.protocols :refer [EditableBackedStructure]]))

;; A persisted var is the 'simplest' base construct
;; in the backing store.  It is essentially just a
;; tuple of key and backing store that can be
;; derefed to realize its value
;;
;; No interfaces are provided for mutation
(defrecord PersistedVar [store ^String k]
  clojure.lang.IDeref
  (deref [_] (deref (vs/get-value @store k)))
  EditableBackedStructure
  (make-editable! [_] (->PersistedAtom store k)))

(prefer-method print-method clojure.lang.IPersistentMap clojure.lang.IDeref)
(prefer-method print-method clojure.lang.IRecord clojure.lang.IDeref)
(prefer-method clojure.pprint/simple-dispatch clojure.lang.IPersistentMap clojure.lang.IDeref)

(defn persisted-var [store k] (->PersistedVar store k))
