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

(ns eva.datom-test
  (:require [clojure.test :refer :all]
            [eva.datom :refer [datom]]))

(deftest test-datom-hash-and-equals
  (are [d1 d2] (and (.equals d1 d2) (.equals d2 d1)
                    (= (.hashCode d1) (.hashCode d2)))
    (datom 1 2 3 1 true) (datom 1 2 3 1 true)
    (datom 1 5 "foo" 1 true) (datom 1 5 "foo" 1 true)
    (datom 1 5 "foo" 1 false) (datom 1 5 "foo" 1 false))

  (are [d1 d2] (not (and (.equals d1 d2) (.equals d2 d1)
                         (= (.hashCode d1) (.hashCode d2))))
    (datom 100 2 3 1 true) (datom 1 2 3 1 true)
    (datom 1 5 "foo" 1 true) (datom 1 5 "foo" 1 false)
    (datom 1 5 "foo" 1 false) (datom 1 5 "bar" 1 false)))
