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

(ns eva.contextual.aspects-test
  (:require
   [clojure.test :refer :all]
   [eva.contextual.core :as c]
   [eva.contextual.config :as config]
   [eva.contextual.metrics :as cm]
   [barometer.core :as m]
   [morphe.core :as d]))


(defn reset-config! [f]
  (config/reset!)
  (f))

(defn reset-metrics! [f]
  (cm/release-metrics)
  (f))

(defn current-context []
  c/*context*)

(defn get-metric-explaination
  [metric-name]
  (some->> metric-name
       (m/get-metric m/DEFAULT)
       (m/explanation)))

(use-fixtures :each reset-config! reset-metrics!)

(deftest context-aspects:context-capturing
  (testing "current context is empty"
    (is (= {} (:lexical (current-context))))
    (is (= {} (:runtime (current-context)))))

  (testing "capturing runtime"
    (d/defn ^{::d/aspects [(c/capture {:a "a"})]}  captures-runtime  []
      (is (not-empty (:lexical (current-context))))
      (is (= {:a "a"} (:runtime (current-context)))))
    (captures-runtime))

  (testing "capturing runtime twice"
    (d/defn ^{::d/aspects [(c/capture {:a "child"})]}  child  []
      (is (= {:a "child"} (:runtime (current-context)))))
    (d/defn ^{::d/aspects [(c/capture {:a "parent"})]} parent []
      (is (= {:a "parent"} (:runtime (current-context))))
      (child))
    (parent))

  (testing "adding context in child"
    (d/defn ^{::d/aspects [(c/capture {:b "b"})]}  child  []
      (is (= {:a "a" :b "b"} (:runtime (current-context)))))
    (d/defn ^{::d/aspects [(c/capture {:a "a"})]} parent  []
      (child))
    (parent))

  (testing "finding non-existent context"
    (d/defn ^{::d/aspects [(c/capture [:b])]} child []
      (is (= {} (:runtime (current-context)))))
    (d/defn ^{::d/aspects [(c/capture {:a "a"})]} parent  []
      (child))
    (parent))

  (testing "finding existent context"
    (d/defn ^{::d/aspects [(c/capture [:a :b])]} child []
      (is (= {:a "a"} (:runtime (current-context)))))
    (d/defn ^{::d/aspects [(c/capture {:a "a"})]} parent  []
      (child))
    (parent)))

(deftest context-aspects:timed
  (testing "does not create context"
    (d/defn ^{::d/aspects [(c/timed {:a "a"})]} timed []
      (is (= {} (:runtime (current-context)))))
    (timed))

  (testing "creates a tagged timer"
    (d/defn ^{::d/aspects [(c/timed {:a "a"})]} timed [])
    (timed)
    (let [metric-name "eva.contextual.aspects-test.timed.timer?a=a"]
      (is (= "Timer for the function: eva.contextual.aspects-test/timed"
             (get-metric-explaination metric-name)))
      (m/remove-metric m/DEFAULT metric-name)))

  (testing "creates a tagged timer by adding tags to existing ones"
    (d/defn ^{::d/aspects [(c/timed {:b "b"})]} child [])
    (d/defn ^{::d/aspects [(c/capture {:a "a"})]} parent [] (child))
    (let [metric-name "eva.contextual.aspects-test.child.timer?a=a&b=b"]
      (parent)
      (is (= "Timer for the function: eva.contextual.aspects-test/child"
             (get-metric-explaination metric-name)))
      (m/remove-metric m/DEFAULT metric-name)))

  (testing "creates a tagged timer by finding parent tags"
    (d/defn ^{::d/aspects [(c/timed [:a :b])]} child [])
    (d/defn ^{::d/aspects [(c/capture {:a "a"})]} parent [] (child))
    (let [metric-name "eva.contextual.aspects-test.child.timer?a=a"]
      (parent)
      (is (= "Timer for the function: eva.contextual.aspects-test/child"
             (get-metric-explaination metric-name)))
      (m/remove-metric m/DEFAULT metric-name)))

  (testing "timer has no tags if no params provided"
    (d/defn ^{::d/aspects [(c/timed)]} child [])
    (d/defn ^{::d/aspects [(c/capture {:a "a"})]} parent [] (child))
    (let [metric-name "eva.contextual.aspects-test.child.timer"]
      (parent)
      (is (= "Timer for the function: eva.contextual.aspects-test/child"
             (get-metric-explaination metric-name)))
      (m/remove-metric m/DEFAULT metric-name))))
