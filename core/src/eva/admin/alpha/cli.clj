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

(ns eva.admin.alpha.cli
  "Entry point for Eva Administrative CLI tooling"
  (:require [eva.admin.alpha.api :as admin-api]
            [clojure.edn :as edn])
  (:import [java.util UUID]))

(defmulti run-command (fn [cmd-str & params] cmd-str))

(defn parse-config [config]
  (-> config edn/read-string))

(defmethod run-command "simple-migration"
  [_ [source destination database-id]]
  (let [id (UUID/fromString database-id)
        src (parse-config source)
        dst (parse-config destination)]
    (admin-api/stateful-migration! src dst id)))

(defn -main [& args]
  ;; TODO: Integrate with clojure.tools.cli for parsing.
  (let [[cmd & params] args]
    (run-command cmd params)))
