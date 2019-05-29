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

(ns eva.v2.storage.value-store.protocols
  (:require [tesserae.impl]) ;; force generation of Tessera type.
  (:import [java.util Map]
           [tesserae.impl.Tessera]))

(definterface IValueStorage
  (^String getPartition [] "Provides the string identifier this value store uses for multi-tenant partitioning.")
  (^tesserae.impl.Tessera getValue [^String k] "Fetches the desired value. Returns a tessera that will contain the value. If an error occurs, the error will be re-thrown on dereference.")
  (^tesserae.impl.Tessera getValues [^Iterable ks] "Fetches the desired values. Returns a tessera that will contain a map k->v. If an error occurs, the error will be re-thrown on dereference.")
  (^tesserae.impl.Tessera createKey [^String k v] "Writes the value v to the key k, provided that the key does not already exist.")
  (^tesserae.impl.Tessera putValue [^String k v] "Stores the provided value under the specified key. Returns a tessera containing true/false for success/failure. If an error occurs, the error will be re-thrown on dereference.")
  (^tesserae.impl.Tessera putValues [^Iterable kvs] "Stores the provided keys and values. Returns a tessera containing a map k->boolean indicating success/failure for each. If an error occurs, the error will be re-thrown on dereference.")
  (^tesserae.impl.Tessera removeKey [^String k] "Removes entry for the key. Returns a tessera containing true/false for success/failure. Removing a non-existent value always succeeds.")
  (^tesserae.impl.Tessera removeKeys [^Iterable ks] "Removes multiple entries. Returns a tessera containing a map k->boolean indicating success/failure for each. Removing a non-existent value always succeeds. If an error occurs, the error will be re-thrown on dereference.")
  (^tesserae.impl.Tessera replaceValue [^String k prev new] "Atomically replaces an existing entry via compare and swap, given expected previous value and provided new value. Returns a Tessera containing true/false for success/failure. If an error occurs, the error will be re-thrown on dereference."))

(definterface ICacheStorage
  (^Iterable getIgnoredKeys [] "Returns a collection of keys ignored by the cache.")
  (addIgnoredKey [^String k] "Adds a key to the set of those ignored by the cache.")
  (removeIgnoredKey [^String k] "Removes a key from the set of those ignored by the cache."))
