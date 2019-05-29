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

(ns eva.query.error
  (:require [eva.error :refer :all]))

(deferror-group parse-err
  (:query.invalid [:expression])
  (find-spec "Invalid find spec")
  (inputs "Invalid inputs" [:invalid])
  (with "Invalid with-clause" [:invalid])
  (clause "Invalid clause")
  (or-clause "Invalid or-clause" [:invalid])
  (or-join "Invalid or-join-clause" [:invalid])
  (where "Invalid where-clause" [:invalid])
  (query "Invalid query form"))

(deferror-group builtin
  :query.builtins
  (function "An error occurred during the execution of a built-in function")
  (aggregate "An error occurred during the execution of a built-in aggregate function"))
