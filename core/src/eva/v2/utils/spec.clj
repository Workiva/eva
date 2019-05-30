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

(ns eva.v2.utils.spec
  (:require [clojure.spec.alpha :as s]
            [eva.error :refer [raise]]))

(defn conform-spec [spec x]
  (let [conformed (s/conform spec x)]
    (if (s/invalid? conformed)
      (raise :invalid-spec
             (str "\n" (s/explain-str spec x))
             {:spec spec
              :x x
              :explain-data (s/explain-data spec x)})
      conformed)))
