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

(ns eva.quartermaster-patches
  (:require [quartermaster.core]
            [eva.contextual.core]
            [clojure.test :refer :all]))

(defmacro testing-for-resource-leaks
  "This macro is for tests only and it is specialization of
    quartermaster's `testing-for-resource-leaks` macro - metrics are allowed to leak:
     1. In most cases metrics get created not through resource manager anyway and
        therefore they are getting released when application quits
     2. When tagged metrics get created for something which is not shared resource they have
        to be released manually (for example, `eva.api/release` must also release metrics and do it
        outside of `morphe.core/defn` macro"
  [& body]
  `(let [leaked# (quartermaster.core/identify-resource-leaks ~@body)]
     (is (empty? (remove #(= `eva.contextual.metrics/metrics %) leaked#)))))

(comment
  (testing-for-resource-leaks (prn "example")))
