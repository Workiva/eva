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

(ns v2.startup-test
  (:require [eva.server.v2 :as v2]
            [clojure.test :refer :all]))

(defn test-init-db [db]
  (let [sleep-time (if (= db 0) 200 1)]
    (Thread/sleep sleep-time)
    (* db 10)))

(deftest unit:uncoordinated-pmap
  (let [started-dbs (#'v2/pmap "test-init-db" test-init-db (range 100))]
    (is (>= (nth started-dbs 0) 10))
    (is (= (nth started-dbs 99) 0)) ;; the element which takes longest time to process does not block other elements
    (is (= (reduce + (range 0 1000 10))
           (reduce + started-dbs)))))
