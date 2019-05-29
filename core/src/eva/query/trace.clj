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

(ns eva.query.trace
  "Tracing utilities to add toggleable tracing to the query engine.

   Enable via setting the config option :eva.query.trace-logging or
   the environment variable EVA_QUERY_TRACE_LOGGING to true."
  (:require [eva.config :refer [config-strict]]
            [recide.sanex.logging :as log]))

(defmacro trace
  [& stuff]
  `(when (config-strict :eva.query.trace-logging)
     (log/trace ~@stuff)))

(defmacro trace-spy
  ([form]
   `(let [r# ~form]
      (trace r#)
      r#))
  ([msg f form]
   `(let [r# ~form]
      (trace ~msg (~f r#))
      r#)))
