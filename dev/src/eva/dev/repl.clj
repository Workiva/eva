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

 (ns eva.dev.repl
   (:require [clojure.tools.namespace.repl :refer [refresh]]
             [eva.error]
             [recide.sanex]
             [eva.api :as eva]))

(alter-var-root #'eva.error/*capture-flag* (constantly true))
(alter-var-root #'recide.sanex/*sanitization-level* (constantly recide.sanex/noop-sanitization))
