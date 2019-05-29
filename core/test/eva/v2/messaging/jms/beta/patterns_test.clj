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

(ns eva.v2.messaging.jms.beta.patterns-test
  (:require [clojure.test :refer :all]
            [eva.test.clojure-test-ext]
            [eva.v2.messaging.jms.beta.core :as jms]
            [eva.v2.messaging.jms.beta.connection-factory :as connection-factory]
            [eva.v2.messaging.jms.beta.patterns :as pattern])
  (:import (javax.jms ConnectionFactory Session ObjectMessage TextMessage)
           (java.util.concurrent ExecutionException)))

(defn setup-requestor-responder [^ConnectionFactory cf message-protocol service-queue service-handler]
  (let [responder-connection (jms/create-connection cf)
        responder (pattern/responder responder-connection
                                     {:request-queue service-queue
                                      :message-protocol message-protocol
                                      :request-handler service-handler})
        requestor-connection (jms/create-connection cf)
        requestor (pattern/requestor requestor-connection
                                     {:request-queue service-queue
                                      :message-protocol message-protocol})]
    (.start responder-connection)
    (.start requestor-connection)
    {:responder responder
     :requestor requestor
     :stop! (fn []
              (.close requestor)
              (.close responder)
              (.stop responder-connection)
              (.stop requestor-connection))}))

(def edn-data+serialized-errors
  (reify pattern/MessageProtocol
    (data-message? [_ msg] (not= ::pattern/error (#'pattern/read-message-type msg)))
    (new-data-message [_ session] (.createTextMessage ^Session session))
    (write-data-message [_ msg data] (.setText ^TextMessage msg (pr-str data)))
    (read-data-message [_ msg] (some-> ^TextMessage msg
                                       (.getText)
                                       (clojure.edn/read-string)))
    (error-message? [_ msg] (= ::pattern/error (#'pattern/read-message-type msg)))
    (new-error-message [_ session] (doto (.createObjectMessage ^Session session)
                                     (#'pattern/set-message-type! ::pattern/error)))
    (write-error-message [_ msg err] (.setObject ^ObjectMessage msg err))
    (read-error-message [_ msg] (.getObject ^ObjectMessage msg))))

(deftest test:requestor-responder-integration
  (let [received-requests (atom [])
        connection-factory (connection-factory/new-connection-factory
                            (connection-factory/connection-factory-name :activemq)
                            "vm://localhost?broker.persistent=false")
        service-handler (fn [req]
                          (swap! received-requests conj req)
                          (when-not (integer? req) (throw (IllegalArgumentException.)))
                          (inc req))
        {:keys [requestor stop!]} (setup-requestor-responder
                                    connection-factory
                                    edn-data+serialized-errors
                                    "test-service"
                                    service-handler)]
    (try
      (is (= 1 @(pattern/send-request! requestor 0)))
      (is (= 2 @(pattern/send-request! requestor 1)))
      (is (= 3 @(pattern/send-request! requestor 2)))
      (is (= [0 1 2] @received-requests))
      (is (thrown-with-cause? ExecutionException IllegalArgumentException
                              @(pattern/send-request! requestor :foo)))
      (is (= [0 1 2 :foo] @received-requests))
      (finally (stop!)))))
