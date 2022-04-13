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

(ns eva.v2.fressian
  "Provides consolidated write & read handlers for use in various circumstances
   depending on the content being handled."
  (:require [eva.v2.storage.value-store.handlers :refer [merge-write-handlers
                                                         merge-read-handlers]]
            [recide.sanex.logging :as log]
            [eva.bytes :refer [bba-write-handler bba-read-handler]]
            [eva.datom :refer [datom-write-handler datom-read-handler]]
            [eva.v2.datastructures.bbtree.fressian :as bbtree])
  (:import (org.fressian.handlers WriteHandler ReadHandler)
           (org.fressian Writer Reader)))

(def eva-only-write-handlers (merge bba-write-handler
                                    bbtree/all-writers
                                    datom-write-handler))

(def eva-only-read-handlers (merge bba-read-handler
                                   bbtree/all-readers
                                   datom-read-handler))

(defrecord UnhandledObject [type])

(def object-writer
  (reify WriteHandler
    (write [_  writer v]
      (log/warn "unhandled fressian Object writer being activated for:" (type v))
      (.writeTag ^Writer writer "unhandled-object" 1)
      (.writeString writer (str (type v))))))

(def object-reader
  (reify ReadHandler
    (read [_ reader _ _]
      (log/warn "unhandled fressian Object reader being activated.")
      (UnhandledObject. (.readObject ^Reader reader)))))

(def eva-write-handlers (merge-write-handlers
                         (merge bba-write-handler
                                bbtree/all-writers
                                datom-write-handler
                                #_(rf/construct-write-handlers eva.error/eva-error-form))))

(def eva-read-handlers (merge-read-handlers
                        (merge bba-read-handler
                               bbtree/all-readers
                               datom-read-handler
                               #_(rf/construct-read-handlers eva.error/eva-error-form))))

(def eva-messaging-write-handlers
  (merge-write-handlers
   (merge bba-write-handler
          bbtree/all-writers
          datom-write-handler
          #_(rf/construct-write-handlers eva.error/eva-error-form)
          #_{Object {"unhandled-object" object-writer}})))

(def eva-messaging-read-handlers
  (merge-read-handlers
   (merge bba-read-handler
          bbtree/all-readers
          datom-read-handler
          #_(rf/construct-read-handlers eva.error/eva-error-form)
          #_{"unhandled-object" object-reader})))
