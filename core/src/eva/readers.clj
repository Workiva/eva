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

;; IMPORTANT: Take care when making changes to this file!
;; The following metadata MUST be present on the namespace declaration:
;;    {:clojure.tools.namespace.repl/unload false
;;     :clojure.tools.namespace.repl/load false}
;; This metadata excludes this namespace from reloading by clojure.tools.namespace.
;;
;; If this file is reloaded by clojure.tools.namespace, the clojure reader functions
;; referenced in clojure.core/*data-reader* WILL BREAK.
;;
;; This is because clojure.tools.namespace completely removes/unloads the
;; namespace-instance, causing the vars in clojure.core/*data-readers* to point to
;; a namespace-instance that no longer corresponds with this file.
;;
;; If you make changes to this file, or any function referenced here, you must
;; restart your repl.
(ns ^{:clojure.tools.namespace.repl/unload false
      :clojure.tools.namespace.repl/load false}
  eva.readers
  (:require [clojure.edn :as edn]
            [eva.functions]
            [eva.entity-id]
            [eva.datom]))

(defn read-db-fn [db-fn-form] (eva.functions/build-db-fn db-fn-form))
(defn read-db-id [vec-form] (apply eva.entity-id/tempid vec-form))

(def reader-functions {'db/fn #'read-db-fn
                       'db/id #'read-db-id})

(defn ensure-parsed
  "For functions that take EDN data structures, this function will attempt to parse
  any passed strings, and will otherwise return non-string values unchanged."
  [x]
  (if (string? x)
    (edn/read-string {:readers reader-functions} x)
    x))
