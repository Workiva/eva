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

(ns eva.contextual.usage-examples
  (:require
   [eva.contextual.core :as c]
   [eva.contextual.config :as config]
   [morphe.core :as d]))

;;-------------------------- Aspects API ------------------------------------

;; "captured" runtime context could be found by other contextual aspect helpers:
(d/defn ^{::d/aspects [(c/capture {:a "a"})
                       (c/traced [:a])  ;; span has tags {:a "a"}
                       (c/logged [:a])  ;; logged with tags {:a "a"}
                       (c/timed [:a])   ;; timer has tags {:a "a"}
                       ]}
  finding-captured
  [c])

;; logs "eva.contextual-test/finding-captured(c)[a=a]"
(finding-captured nil)



;; each aspect could customize runtime context:
;;   (c/aspect [...]) - find in existing runtime context
;;   (c/aspect {...}) - merge with existing runtime context
(d/defn ^{::d/aspects [(c/capture {:a "a"})
                       (c/traced [:a :b]) ;; span has tags {:a "a"} (no :b)
                       (c/logged)         ;; no tags
                       (c/timed)          ;; timer has tags {:a "a"}
                       ]}
  customly-aspected
  [c])

;; logs "eva.contextual-test/customly-aspected(c)"
(customly-aspected nil)



;; (c/capture {...}) *merges* with existing runtime context
(d/defn ^{::d/aspects [(c/capture {:a "b" :b "b"})
                       (c/logged)]}
  merged [c])

(d/defn ^{::d/aspects [(c/capture {:a "a" :c "c"})
                       (c/logged)]}
  call-merged [c]
  (merged c))

;; logs "eva.contextual-test/merged(c)[a=b&b=b&c=c]"
(call-merged nil)




;; (c/capture [...]) *finds* existing tags
(d/defn ^{::d/aspects [(c/capture [:a :b])
                       (c/logged)]}
  find [c])

(d/defn ^{::d/aspects [(c/capture {:a "a" :b "b" :c "c"})]}
  call-find [c]
  (find c))

;; logs "eva.contextual-test/find(c)[a=a&b=b]"
(call-find nil)




;; (c/capture {...}) and (c/capture [...]) could be combined
(d/defn ^{::d/aspects [(c/capture [:a :b])
                       (c/capture {:d "d"})
                       (c/logged)]}
  combined [c])

(d/defn ^{::d/aspects [(c/capture {:a "a" :b "b" :c "c"})
                       (c/logged)]}

  call-combined [c]
  (combined c))

;; logs "eva.contextual-test/combined(c)[a=a&b=b&d=d]"
(call-combined nil)




;; c/capture might be not present
(d/defn ^{::d/aspects [(c/logged {:a "a"})]}
  nothing-captured [c])

;; logs "eva.contextual-test/nothing-captured(c)[a=a]"
(nothing-captured nil)




;; params value could be used as tag values
(d/defn ^{::d/aspects [(c/logged '{:c c})]}
  param-value-in-tag [c])

;; logs "eva.contextual-test/param-value-in-tag(c)[c=something-interesting]"
(param-value-in-tag "something-interesting")


;;; ------------ API for using within the function body --------------------

;; default behavior
(d/defn ^{::d/aspects [(c/capture {:a "a"})]}
  whatever-captured [c]
  ;; logs "eva.contextual-test/whatever-captured(c)[a=a] warning"
  (c/log :warn "warning")

  ;; increments "eva.contextual-test.whatever-captured.counter?a=a"
  (c/inc))

(whatever-captured nil)

(comment
  ;; overriding per block
  (d/defn ^{::d/aspects [(c/capture {:a "A"})]}
    override-tags []
    (c/override-tags
     {:b "B"}
     ;; logs with 'eva.contextual-test/override-tags{:b B}' prefix
     (c/log :warn "warning")))
  )



;;; ------------------- Config API ------------------------------

;; runtime tags could be disabled via config
;; logs "eva.contextual-test/default-option(c)[a=a]"
(do
  (config/reset!)
  ((d/defn ^{::d/aspects [(c/logged {:a "a"})]}
     default-option [c]) nil))



;; :a tag is ignored
;; logs "eva.contextual-test/ignored(c)"
(do
  (config/reset!)
  (config/disable! :a)
  ((d/defn ^{::d/aspects [(c/logged {:a "a"})]}
     ignored [c]) nil))


;; :a tag could be overriden
;; logs "eva.contextual-test/overriden(c)[a=a]"
(do
  (config/reset!)
  (config/disable! :a)
  ((d/defn ^{::d/aspects [(c/logged ::config/override {:a "a"})]}
     overriden [c]) nil))
