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

(ns eva.v2.datastructures.atom
  (:require [eva.v2.storage.value-store :as vs]
            [eva.datastructures.protocols :refer [BackedStructure]])
  (:import [eva.v2.storage.value_store.protocols IValueStorage]))

;; Wraps a store and a particular key to create a
;; 'persisted atom'.  The atom should behave the same with
;; compare-and-set!, reset!, and deref, with the caveat
;; there may be io delays in performing the operations.
;; these operations *are* blocking.  (this could be
;; abstracted away through a clone of the IAtom interface
;; in the future, if desired.)
(defrecord PersistedAtom [^IValueStorage store ^String k]
  clojure.lang.IAtom
  (compareAndSet [_ oldv newv] @(vs/replace-value @store k oldv newv))
  (reset [_ newval] @(vs/put-value @store k newval))
  clojure.lang.IDeref
  (deref [_] (deref (vs/get-value @store k)))
  BackedStructure
  (storage-id [_] k)
  (store [_] store)
  (persisted? [_] true))

(defn persisted-atom [store k]
  (->PersistedAtom store k))

(defn create-persisted-atom
  ([store v]
   (let [k (str (java.util.UUID/randomUUID))]
     @(vs/put-value @store k v)
     (->PersistedAtom store k))))
