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

(ns eva.print-ext
  "Central namespace for specifying clojure print behaviors on the
   top-level java Eva interfaces."
  (:require [clojure.pprint :as pp]
            [quartermaster.core :as qu])
  (:import (eva Connection Database Entity Log Datom)
           (java.io Writer)))

;; Print Implementations for API-Exposed Objects
(defmethod print-method Database
  [db ^Writer w]
  (.write w (str "#DB[" (-> db :log-entry :tx-num) "]")))

(defmethod pp/simple-dispatch Database
   [^Database db]
   (print db))

(prefer-method print-method Database clojure.lang.IPersistentMap)
(prefer-method print-method Database clojure.lang.IRecord)
(prefer-method print-method Database java.util.Map)
(prefer-method pp/simple-dispatch Database clojure.lang.IPersistentMap)

(defmethod print-method Connection [conn ^java.io.Writer w]
  (.write w (str "#Connection{:version 1, :status " (qu/status conn)"}")))

(defmethod pp/simple-dispatch Connection [conn]
  (print conn))

(prefer-method print-method Connection clojure.lang.IPersistentMap)
(prefer-method print-method Connection clojure.lang.IRecord)
(prefer-method print-method Connection java.util.Map)
(prefer-method pp/simple-dispatch Connection clojure.lang.IPersistentMap)

(defmethod pp/simple-dispatch Log
  [^Log log]
  (print log))

(prefer-method pp/simple-dispatch Log clojure.lang.IPersistentMap)

(defmethod print-method Datom [^Datom d ^Writer writer]
  (.write writer "#datom[")
  (print-method (.e d) writer)
  (.write writer " ")
  (print-method (.a d) writer)
  (.write writer " ")
  (print-method (.v d) writer)
  (.write writer " ")
  (print-method (.tx d) writer)
  (.write writer " ")
  (print-method (.added d) writer)
  (.write writer "]"))

(prefer-method print-method Datom clojure.lang.IRecord)
(prefer-method print-method Datom clojure.lang.IPersistentMap)
(prefer-method print-method Datom java.util.Map)

(defmethod pp/simple-dispatch Datom
  [^Datom d]
  (pp/pprint-logical-block :prefix "#datom"
                            :suffix ""
                            (pp/write-out [(.e d) (.a d) (.v d) (.tx d) (.added d)])))

(prefer-method pp/simple-dispatch Datom clojure.lang.IRecord)
(prefer-method pp/simple-dispatch Datom clojure.lang.IPersistentMap)
(prefer-method pp/simple-dispatch Datom java.util.Map)
