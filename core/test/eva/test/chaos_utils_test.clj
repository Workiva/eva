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

(ns eva.test.chaos-utils-test
  (:require [eva.v2.storage.value-store :as vs]
            [eva.v2.storage.core :as bs]
            [eva.test.chaos-utils :as chaos-utils]
            [eva.v2.storage.testing-utils :refer [with-mem-value-store with-mem-block-store]]
            [clojure.data.generators :as dg]
            [clojure.test :refer :all])
  (:import (eva ByteString)))

(defn throwing-op [& args]
  (throw (Exception. "something-bad-has-happened")))

(deftest chaos-utils:value-store-put-value-fails-100pct
  (testing "put-value always fail"
    (with-mem-value-store value-store
      (chaos-utils/with-induced-chaos {vs/put-value throwing-op}
        (dotimes [_ 10]
          (is (thrown-with-msg? Exception #"something-bad-has-happened" @(vs/put-value @value-store "0" "value"))))))))

(defmacro validate-some-failures-happen
  "This test sets ~method to fail in 50% cases however it does not verify an actual rate
   since that would be same as finding the expected value of a random variable with some confidence interval
   and it would be a more complex test.
   Instead it verifies if there were just *some* failures. There is still very small (0.5^100 = 8e-31)
   chance of this test to fail (i.e. ~method must succeed 100 times in row with possibility of 0.5 to
   fail on every attempt) which should be very, very rare."
  [method body]
  `(let  [times-thrown# (atom 0)]
     (chaos-utils/with-induced-chaos {~method [[5 throwing-op] [5 ~method]]}
       (dotimes [_# 100]
         (try ~body
              (catch Exception _1#
                (swap! times-thrown# inc)))))
     (is (pos? @times-thrown#))
     (is (< @times-thrown# 100))))

(deftest chaos-utils:value-store-put-value-sometimes-fails
  (with-mem-value-store value-store
    (validate-some-failures-happen
     vs/put-value
     @(vs/put-value @value-store "0" "value"))))

(deftest chaos-utils:block-storage-write-block-sometimes-fails
  (let  [block (bs/->Block "namespace" "block-id" {} (ByteString/copyFromUTF8 "hello world!"))]
    (with-mem-block-store block-store
      (validate-some-failures-happen
       bs/write-block
       (bs/write-block @block-store :write-full block)))))

(deftest chaos-utils:value-store-put-value-get-value-succeed-100pct
  (testing "put-value and get-value always succeed"
    ;; note, the 'always-succeed' could take different forms:
    (with-mem-value-store value-store
      (chaos-utils/with-induced-chaos {vs/put-value [[10 vs/put-value]] vs/get-value vs/get-value}
        (dotimes [_ 10]
          (let [value (dg/string)]
            @(vs/put-value @value-store "0" value)
            (is (= value @(vs/get-value @value-store "0")))))))))

(deftest chaos-utils:value-store-does-not-allow-unrecognized-keys
  (testing "mocking non-existent method should fail"
    (with-mem-value-store value-store
      (is (thrown-with-msg? Exception #"non-existent-mehtod" (eval `(chaos-utils/with-induced-chaos {eva.v2.storage.value-store/non-existent-mehtod vs/get-value})))))))
