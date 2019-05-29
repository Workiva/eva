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

(ns eva.utils.unify)

(defn walk*
  "Walks a substitution-map s, resolving all intermediate lvars
  (as identified by pred lvar?), until a terminal value is found."
  [s u lvar?]
  (if-let [pr (s u)]
    (if (lvar? pr)
      (recur s pr lvar?)
      pr)
    u))

(defn unify* [u v s lvar?]
  (let [u (walk* s u lvar?)
        v (walk* s v lvar?)]
    (cond (and (lvar? u)
               (lvar? v)
               (= u v)) s
          (lvar? u) (assoc s u v)
          (lvar? v) (assoc s v u)
          :else (or (and (= u v) s)
                    nil))))

(defprotocol Unifications
  (unifications [x source subst lvar?]
    "given source, previous substitition, and pred to detect lvars, returns sequence of possible unifications"))
