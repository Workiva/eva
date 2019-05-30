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

(ns eva.v2.messaging.node.alpha-beta-interop-test
  (:require [clojure.test :refer :all]
            [eva.v2.messaging.node.alpha :as node-alpha]
            [eva.v2.messaging.node.beta :as node-beta]
            [eva.v2.messaging.jms.alpha.local-broker :as local-broker]
            [eva.v2.system.protocols :as system-protocols]
            [quartermaster.core :as qu]))


(deftest alpha<->beta-communication-over-artemis
  (let [broker-uri (local-broker/broker-uri)]
    (try
      (qu/acquiring [alpha (qu/acquire node-alpha/messenger-nodes
                                       :alpha {:broker-uri broker-uri})
                     beta (qu/acquire node-beta/messenger-nodes
                                      :beta {:broker-uri broker-uri})]
                    (try
                      (testing "requestor-responder compatibility"
                        (let [alpha-responder (system-protocols/open-responder! @alpha
                                                                                "test.service.alpha"
                                                                                (fn [data]
                                                                                  [:test.service.alpha :received data])
                                                                                {})
                              beta-responder (system-protocols/open-responder! @beta
                                                                               "test.service.beta"
                                                                               (fn [data]
                                                                                 [:test.service.beta :received data])
                                                                               {})
                              alpha->alpha-requestor (system-protocols/open-requestor! @alpha "test.service.alpha" {})
                              beta->alpha-requestor (system-protocols/open-requestor! @beta "test.service.alpha" {})

                              alpha->beta-requestor (system-protocols/open-requestor! @alpha "test.service.beta" {})
                              beta->beta-requestor (system-protocols/open-requestor! @beta "test.service.beta" {})]
                          (try
                            (is (= [:test.service.alpha :received "test-data"]
                                   @(system-protocols/request! @alpha "test.service.alpha" "test-data")
                                   @(system-protocols/request! @beta "test.service.alpha" "test-data")
                                   ))
                            (is (= [:test.service.beta :received "test-data"]
                                   @(system-protocols/request! @alpha "test.service.beta" "test-data")
                                   @(system-protocols/request! @beta "test.service.beta" "test-data")))

                            (finally
                              (system-protocols/close-responder! @beta "test.service.beta")
                              (system-protocols/close-responder! @alpha "test.service.alpha")
                              (system-protocols/close-requestor! @beta "test.service.beta")
                              (system-protocols/close-requestor! @beta "test.service.alpha")
                              (system-protocols/close-requestor! @alpha "test.service.beta")
                              (system-protocols/close-responder! @alpha "test.service.alpha")))))

                      (testing "publisher-subscriber compatibility"
                        (let [alpha-publisher (system-protocols/open-publisher! @alpha "test.publish.alpha" {})
                              beta-publisher (system-protocols/open-publisher! @beta "test.publish.beta" {})
                              received (atom {:alpha->alpha #{}
                                              :alpha->beta #{}
                                              :beta->alpha #{}
                                              :beta->beta #{}})
                              alpha-subscriber-to-alpha-pub (system-protocols/subscribe! @alpha
                                                                                         :alpha->alpha
                                                                                         "test.publish.alpha"
                                                                                         (fn [data]
                                                                                           (swap! received update :alpha->alpha conj data))
                                                                                         {})
                              alpha-subscriber-to-beta-pub (system-protocols/subscribe! @alpha
                                                                                        :beta->alpha
                                                                                        "test.publish.beta"
                                                                                        (fn [data]
                                                                                          (swap! received update :beta->alpha conj data))
                                                                                        {})
                              beta-subscriber-to-alpha-pub (system-protocols/subscribe! @beta
                                                                                        :alpha->beta
                                                                                        "test.publish.alpha"
                                                                                        (fn [data]
                                                                                          (swap! received update :alpha->beta conj data))
                                                                                        {})
                              beta-subscriber-to-beta-pub (system-protocols/subscribe! @beta
                                                                                       :beta->beta
                                                                                       "test.publish.beta"
                                                                                       (fn [data]
                                                                                         (swap! received update :beta->beta conj data))
                                                                                       {})]
                          (try
                            (system-protocols/publish! @alpha "test.publish.alpha" "to test.publish.alpha")
                            (system-protocols/publish! @beta "test.publish.beta" "to test.publish.beta")
                            (Thread/sleep 100)
                            (is (= {:alpha->alpha #{"to test.publish.alpha"}
                                    :alpha->beta  #{"to test.publish.alpha"}
                                    :beta->alpha #{"to test.publish.beta"}
                                    :beta->beta #{"to test.publish.beta"}}
                                   @received))
                            (finally
                              (system-protocols/unsubscribe! @alpha :alpha->alpha "test.publish.alpha")
                              (system-protocols/unsubscribe! @alpha :beta->alpha "test.publish.beta")
                              (system-protocols/unsubscribe! @beta :alpha->beta "test.publish.alpha")
                              (system-protocols/unsubscribe! @beta :beta->beta "test.publish.beta")
                              (system-protocols/close-publisher! @alpha "test.publish.alpha")
                              (system-protocols/close-publisher! @beta "test.publish.beta")))))
                      (finally
                        (qu/release alpha)
                        (qu/release beta))))
      (finally
        (local-broker/stop-broker!)))))
