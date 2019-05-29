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

(ns eva.api.java.util
  "Supporting implementation of functions
  in eva.Util class"
  (:refer-clojure :exclude [read])
  (:require [clojure.edn :as edn]
            [eva.entity-id]
            [eva.datom]
            [clojure.java.io :as io]
            [utiliva.core :as uc]
            [eva.readers :refer [reader-functions]])
  (:import (java.io Closeable PushbackReader BufferedReader)
           (java.util.function Function)))

(defn Function->IFn [^Function function]
  (reify clojure.lang.IFn
    (invoke [_ x] (.apply function x))))

(defn read
  ([s]
   (read {} s))
  ([readers s]
   (let [symboled-map (into {} (comp (uc/map-keys symbol)
                                     (uc/map-vals Function->IFn))
                            readers)]
     (edn/read-string {:readers (merge symboled-map reader-functions)} s))))

(defn read-seq
  "Loads an EDN stream as a lazy sequence.
  No operations occur on the Reader until an item of the sequence is accessed."
  ([rdr] (read-seq {} rdr))
  ([opts rdr]
   (lazy-seq (let [opts (cond-> opts (not (contains? opts :eof)) (assoc :eof ::eof))
                   val (edn/read opts rdr)]
               (when (not= val (:eof opts))
                 (cons val (read-seq opts rdr)))))))

(defn read-all [source]
  (with-open [^Closeable pb-rdr (cond
                                  (instance? PushbackReader source) source
                                  (instance? BufferedReader source) (PushbackReader. source)
                                  :else (PushbackReader. (io/reader source)))]
    (doall (read-seq {:readers reader-functions} pb-rdr))))
