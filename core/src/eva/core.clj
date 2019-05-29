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

(ns eva.core
  (:require [eva.error :as e])
  (:refer-clojure :exclude [filter]))

(defprotocol Log
  "The Log API protocol"
  (tx-range [log start-t end-t] "returns the range of transactions in log.")
  (update-log-entry-index [log tx-num index-name index-root])
  (update-log-entry-indexes [log tx-num index-roots]))

(defprotocol LogEntry
  (entry->datoms [entry] [entry index])

  (packed-datoms [entry])
  (packed-ref-type-datoms [entry])
  (packed-non-byte-datoms [entry])

  (cur-tx-eid [entry])
  (cur-max-id [entry])
  (index-roots [entry])
  (tx-num [entry]))

(defprotocol IndexSync
  (force-index-sync [log] [log index-name]))

(defprotocol AdvanceIndex
  (safe-advance-index [index db tx-log-entry])
  (advance-index-to-tx [index db tx-log target-tx-num])

  (safe-advance-index-better [index ref-attr-eids byte-attr-eids tx-log-entry])
  (batch-advance-index [index tx-log tx-num])

  (initialize-index [index init-datoms])
  (advance [index db tx-log-entry])
  (->root  [index]))

(defprotocol ComponentResolution
  (resolve-components [db index components])
  (batch-resolve-components [db index component-coll]))

;; Datom Selection Protocols
(defprotocol SelectDatoms
  (select-datoms [source criteria]))

(defprotocol MultiSelectDatoms
  (multi-select-datoms [source criteria] "[source [:index & coll-of-components]]")
  (multi-select-datoms-ordered [source criteria] "[source [:index & coll-of-components]]"))

;; defines a wildcard for datoms-based selection.
(def wildcard '*)
(def wildcard? #{wildcard})

(defprotocol DatabaseInternal
  (db-fn? [db eid])
  (->fn [db eid])
  (allocated-entity-id? [db eid])
  (resolve-eid-partition [db eid]))

(defprotocol LookupRefCache
  (update-lookup-ref-cache! [cache resolved-id-map]))

(defprotocol LookupRefResolution
  (assert-conformant-lookup-ref [resolver ^java.util.List lookup-ref]
    "Returns true if the lookup-ref is structurally sound. If not, throws :resolve-entid/malformed-lookup-ref")
  (resolve-lookup-ref [resolver lookup-ref]
    "If an entity-id exists for the lookup-ref, return it, else nil.")
  (resolve-lookup-ref-strict [resolver lookup-ref]
    "Like resolve-lookup-ref, but throws :resolve-entid/no-such-eid if there is no entity id for the lookup-ref")
  (batch-resolve-lookup-refs [resolver lookup-refs]
    "Resolves a group of lookup refs in aggregate.")
  (batch-resolve-lookup-refs-strict [db lookup-refs]
    "Like batch-resolve-lookup-refs, but throws :resolve-entid/no-such-eid if any of lookup-refs not found"))

(defprotocol EntidCoercionType
  (entid-coercion-type [x] "Returns a keyword representing the type of this object for the purposes of entity id coercion. Exists as a protocol so that we can use satisfies? to identify objects outside of this class. Is also used internally for partitioning objects for batch resolution."))

(e/deferror-group entid-coercion
  :entid-coercion
  (malformed-lookup-ref "Malformed lookup reference")
  (illegal-type "Invalid type for entity coercion")
  (strict-coercion-failure "Failed to strictly coerce to an entity id"))

(e/deferror-group ident-coercion
  :ident-coercion
  (illegal-type "Invalid type for ident coercion")
  (strict-coercion-failure "Failed to strictly coerce to an ident"))
