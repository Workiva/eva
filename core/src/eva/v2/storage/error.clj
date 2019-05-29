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

(ns eva.v2.storage.error
  (:require [eva.error :refer [deferror deferror-group]]))

(deferror data-err
  :storage.error/data
  "Invalid data" [:invalid])

(deferror-group sql-err
  :storage.sql
  (unknown "Unknown exception logged in SQL Storage error")
  (non-extant "SQL Storage not started")
  (cas-failure "SQL Storage CAS failed")
  (unexpected-cas-update-result "SQL Storage CAS update error"))

(deferror request-cardinality
  :storage.error/request-cardinality-exceeded
  "Max request cardinality exceeded")

(deferror-group ddb-err
  :storage.dynamo
  (credentials "Invalid AWSCredentials")
  (unprocessed "Error processing items"))

(deferror-group conc-err
  :value-store.concurrent
  (state "Value store in illegal state")
  (unknown "Value store encountered unknown exception" [:method]))

(deferror npe
  :value-store/NPE
  "Null pointer error in storage" [:method])
