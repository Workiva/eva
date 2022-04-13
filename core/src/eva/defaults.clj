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

(ns eva.defaults
  (:require [again.core :as again]))

(defn default-strategy [retries]
  (cons 0
        (again/max-retries retries
                           (again/randomize-strategy 0.5
                                                     (again/multiplicative-strategy 10 2)))))

(def read-index-retry-strategy #(default-strategy 3))

(def read-log-range-retry-strategy #(default-strategy 3))

(def open-index-retry-strategy #(default-strategy 3))

(def init-fill-index-retry-strategy #(default-strategy 3))

(def write-tx-log-retry-strategy #(default-strategy 3))

(def create-index-retry-strategy #(default-strategy 3))
