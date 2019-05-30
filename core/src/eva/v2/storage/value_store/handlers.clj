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

(ns eva.v2.storage.value-store.handlers
  (:require [eva.v2.storage.error :refer [raise-data-err]]
            [recide.sanex :as sanex]
            [clojure.data.fressian :as fressian])
  (:import (org.fressian.handlers ILookup)))

(defn ilookup? [x] (instance? ILookup x))

(def read-handlers
  (-> fressian/clojure-read-handlers
      fressian/associative-lookup))

(defn merge-read-handlers
  [handlers]
  (-> (merge handlers fressian/clojure-read-handlers)
      fressian/associative-lookup))

(def write-handlers
  (-> fressian/clojure-write-handlers
      fressian/associative-lookup
      fressian/inheritance-lookup))

(defn merge-write-handlers
  [handlers]
  (-> (merge handlers fressian/clojure-write-handlers)
      fressian/associative-lookup
      fressian/inheritance-lookup))

(defn normalize-read-handlers
  ([x] (normalize-read-handlers x false))
  ([x strict?]
   (cond (ilookup? x) x
         (map? x) (merge-read-handlers x)
         :else (when (#{true :strict} strict?)
                 (raise-data-err (str "normalize-read-handlers passed unknown value: " (pr-str x))
                                 {:invalid x,
                                  ::sanex/sanitary? true})))))

(defn normalize-write-handlers
  ([x] (normalize-write-handlers false x))
  ([x strict?]
   (cond (ilookup? x) x
         (map? x) (merge-write-handlers x)
         :else (when (#{true :strict} strict?)
                 (raise-data-err (str "normalize-write-handlers passed unknown value: " (pr-str x))
                                 {:invalid x,
                                  ::sanex/sanitary? true})))))
