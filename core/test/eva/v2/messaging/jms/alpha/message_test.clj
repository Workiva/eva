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

(ns eva.v2.messaging.jms.alpha.message-test
  (:require [clojure.test :refer :all]
            [clojure.spec.test.alpha :as stest]
            [eva.v2.messaging.jms.alpha.message :as message]
            [recide.utils :as ru]
            [eva.error :as eva-err])
  (:import (com.mockrunner.mock.jms MockBytesMessage)
           (eva.error.v1 EvaException)
           (clojure.lang IExceptionInfo)))

(defn ^:private instrumented [f]
  (stest/instrument (stest/enumerate-namespace 'eva.v2.messaging.jms.alpha.message))
  (try (f)
       (finally (stest/unstrument (stest/enumerate-namespace 'eva.v2.messaging.jms.alpha.message)))))

(use-fixtures :once instrumented)

(deftest internal-message-encoding-and-decoding
  (testing "message encoding"
    (testing "success"
      (testing "with data payload"
        (let [msg (MockBytesMessage.)
              result (#'message/encode! msg [1 2 3])]
          (.setReadOnly msg true)
          (is (contains? result ::message/data-body))
          (is (= {:throwable? false :payload [1 2 3]} (::message/data-body result)))
          (is (::message/write-body-success? result))
          (is (identical? msg (::message/encoded-message result)))
          (is (= (count (::message/encoded-body result))
                 (.getBodyLength msg)))))
      (testing "with throwable payload"
        (let [msg (MockBytesMessage.)
              result (#'message/encode! msg (eva-err/error :foo/bad "test error" {::test ::error}))]
          (.setReadOnly msg true)
          (is (contains? result ::message/throwable-body))
          (is (true? (get-in result [::message/throwable-body :throwable?])))
          (is (bytes? (get-in result [::message/throwable-body :payload])))
          (is (= "test error" (.getMessage (ru/deserialize-throwable (get-in result [::message/throwable-body :payload])))))
          (is (= {::test ::error
                  :eva/error :foo/bad}
                 (dissoc (ex-data (ru/deserialize-throwable (get-in result [::message/throwable-body :payload])))
                         :recide/error)))
          (is (::message/write-body-success? result))
          (is (identical? msg (::message/encoded-message result))))))
    (testing "fressian encoding failure"
      (let [msg (MockBytesMessage.)
            result (#'message/encode! msg (reify clojure.lang.IDeref))]
        (is (not (::message/encode-body-success? result)))
        (is (= ::message/encode-body-error (::message/encode-body-result result)))
        (is (instance? IllegalArgumentException (::message/encode-body-error result)))
        (is (not (::message/write-body-success? result)))))
    (testing "message write failure"
      (let [msg (doto (MockBytesMessage.)
                  (.setReadOnly true))
            result (#'message/encode! msg [1 2 3])]
        (is (not (::message/write-body-success? result)))
        (is (= ::message/write-body-error (::message/write-body-result result)))
        (is (instance? Throwable (::message/write-body-error result)))))))

(deftest public-read-write-functions
  (testing "write-content! error cases"
    ;; TODO
    (is (thrown-with-msg? EvaException #"Error transcribing payload"
                          (message/write-content! (MockBytesMessage.)
                                                  (eva-err/error :foo/bad "test message" {::unserializable (reify clojure.lang.IDeref)}))))
    (is (thrown-with-msg? EvaException #"Error encoding message body"
                          (message/write-content! (MockBytesMessage.)
                                                  (reify clojure.lang.IDeref))))

    (is (thrown-with-msg? EvaException #"Error writing message body"
                          (message/write-content! (doto (MockBytesMessage.) (.setReadOnly true))
                                                  [1 2 3]))))
  (testing "writing and reading"
    (testing "data"
      (let [msg (MockBytesMessage.)]
        (is (identical? msg (message/write-content! msg [1 2 3])))
        (.reset msg)
        (.setReadOnly msg true)
        (is (= [1 2 3] (message/read-content msg)))))
    (testing "throwable"
      (let [msg (MockBytesMessage.)]
        (is (identical? msg (message/write-content! msg (eva-err/error :foo/bad "test" {::test ::error}))))
        (.reset msg)
        (.setReadOnly msg true)
        (is (instance? IExceptionInfo (message/read-content msg)))))))
