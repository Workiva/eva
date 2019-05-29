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

(ns eva.contextual.config-test
  (:require
   [clojure.test :refer :all]
   [eva.contextual.config :as config]))

(defn reset-config! [f]
  (config/reset!)
  (f))

(use-fixtures :each reset-config!)

(deftest unit:tags-overriding
  (testing "overriden tags are not ignored when not configured"
    (is (config/keep? :a ::config/override)))

  (testing "overriden tags are not ignored when enabled"
    (config/enable! :a)
    (is (config/keep? :a ::config/override)))

  (testing "overriden tags are not ignored when disabled"
    (config/disable! :a)
    (is (config/keep? :a ::config/override))))


(deftest unit:enabling-tags
  (testing "tags are not ignored when not configured"
    (is (config/keep? :a ::config/as-per-config)))

  (testing "tags are ignored when disabled"
    (config/disable! :a)
    (is (not (config/keep? :a ::config/as-per-config))))

  (testing "tags are not ignored when enabled"
    (config/enable! :a)
    (is (config/keep? :a ::config/as-per-config))))
