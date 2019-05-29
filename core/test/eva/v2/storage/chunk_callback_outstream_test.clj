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

(ns eva.v2.storage.chunk-callback-outstream-test
  (:require [clojure.test :refer :all])
  (:import (eva.storage ChunkCallbackOutputStream)))

(deftest test:ChunkCallbackOutputStream
  (let [chunks (atom [])
        closed? (atom false)]
    (with-open [out (ChunkCallbackOutputStream. 10
                                                (fn on-chunk [ba] (swap! chunks conj ba))
                                                (fn on-close [] (swap! closed? not)))]
      (doseq [i (range 95)]
        (is (not @closed?))
        (.write out (int i))))

    (is (true? @closed?))
    (is (= 10 (count @chunks)))))
