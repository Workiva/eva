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

(ns eva.datastructures.versioning
  (:require [eva.datastructures.protocols :as dsp]
            [recide.sanex :as sanex]
            [eva.error :refer [raise insist]]))

(defmulti version-conversion
  "Takes the version to ensure, a node-or-pointer, whatever extra arguments are
  necessary to facilitate the conversion in whatever context it is being converted.
  It is *unnecessary* to define a method to 'convert' a version to itself.
   (defmethod ensure-version
     [to-version from-version]
     conversion:to-version->from-version ;; name it!
     [_ from-version-node-or-pointer & {:as contextual-args}]
     (conversion-code-here))"
  (fn [version datastructure _] [version (dsp/get-version datastructure)]))

(defmethod version-conversion
  :default
  version-conversion:error
  [v o _]
  (raise :eva.datastructures.versioning/impossible-conversion
         (format "Undefined conversion: %s to %s." (dsp/get-version o) v)
         {::sanex/sanitary? true}))

(defn ensure-version
  "Takes the version to ensure, a node-or-pointer,
  and whatever extra arguments are necessary to facilitate
  the conversion. It is unnecessary to define a method to
  convert a version to itself.
   (defmethod version-conversion
     [to-version from-version]
     conversion:to-version->from-version ;; name it please!
     [_ from-version-node-or-pointer & {:as contextual-args}]
     (conversion-code-here))"
  [version datastructure & {:as args}]
  (insist (satisfies? dsp/Versioned datastructure)
          "ensure-version must be called on Versioned objects.")
  (let [existing (dsp/get-version datastructure)]
    (if (= version existing)
      datastructure
      (version-conversion version datastructure args))))
