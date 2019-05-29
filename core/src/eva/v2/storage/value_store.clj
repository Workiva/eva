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

(ns eva.v2.storage.value-store
  (:require [eva.v2.storage.value-store.protocols]) ;; to force loading for the import
  (:import [eva.v2.storage.value_store.protocols IValueStorage ICacheStorage]
           [java.util Map]))

(defn get-partition
  "Provides the string identifier this value store uses for multi-tenant partitioning."
  [^IValueStorage value-store] (.getPartition value-store))

(defn get-value
  "Fetches the desired value. Returns a tessera that will contain the value. If an
  error occurs, the error will be re-thrown on dereference."
  [^IValueStorage value-store ^String k] (.getValue value-store k))

(defn get-values
  "Fetches the desired values. Returns a tessera that will contain a map k->v. If
  an error occurs, the error will be re-thrown on dereference."
  [^IValueStorage value-store ^Iterable ks] (.getValues value-store ks))

(defn put-value
  "Stores the provided value under the specified key. Returns a tessera containing
  true/false for success/failure. If an error occurs, the error will be re-thrown
  on dereference."
  [^IValueStorage value-store ^String k v] (.putValue value-store k v))

(defn put-values
  "Stores the provided keys and values. Returns a tessera containing a map k->boolean
  indicating success/failure for each. If an error occurs, the error will be re-thrown
  on dereference."
  [^IValueStorage value-store ^java.util.Map kvs] (.putValues value-store kvs))

(defn create-key
  "Writes the value v to the key k, provided that the key does not already exist."
  [^IValueStorage value-store ^String k v] (.createKey value-store k v))

(defn remove-key
  "Removes entry for the key. Returns a tessera containing true/false for
  success/failure. Removing a non-existent value always succeeds."
  [^IValueStorage value-store ^String k] (.removeKey value-store k))

(defn remove-keys
  "Removes multiple entries. Returns a tessera containing a map k->boolean indicating
  success/failure for each. Removing a non-existent value always succeeds. If an error
  occurs, the error will be re-thrown on dereference."
  [^IValueStorage value-store ^Iterable ks] (.removeKeys value-store ks))

(defn replace-value
  "Atomically replaces an existing entry via compare and swap, given expected previous
  value and provided new value. Returns a Tessera containing true/false for
  success/failure. If an error occurs, the error will be re-thrown on dereference."
  [^IValueStorage value-store ^String k old new] (.replaceValue value-store k old new))

(defn get-ignored-keys
  "Returns a collection of keys ignored by the cache (if applicable)."
  [value-store]
  (if (instance? ICacheStorage value-store)
    (.getIgnoredKeys ^ICacheStorage value-store)
    #{}))

(defn add-ignored-key!
  "Adds a key to the set of those ignored by the cache."
  [value-store ^String k]
  (if (instance? ICacheStorage value-store)
    (.addIgnoredKey ^ICacheStorage value-store k)
    true))

(defn remove-ignored-key!
  "Removes a key from the set of those ignored by the cache."
  [value-store ^String k]
  (if (instance? ICacheStorage value-store)
    (.removeIgnoredKey ^ICacheStorage value-store k)
    true))
