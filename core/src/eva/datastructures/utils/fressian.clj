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

(ns eva.datastructures.utils.fressian
  (:require [eva.error :refer [insist]]))

(defprotocol Autological
  (get-var [this] "Returns the var that refers to this function."))

(defrecord AutologicalFunction
     [^clojure.lang.IFn f serial]
   clojure.lang.IFn
   (invoke [_] (f))
   (invoke [_ arg0] (f arg0))
   (invoke [_ arg0 arg1] (f arg0 arg1))
   (invoke [_ arg0 arg1 arg2] (f arg0 arg1 arg2))
   (invoke [_ arg0 arg1 arg2 arg3] (f arg0 arg1 arg2 arg3))
   (invoke [_ arg0 arg1 arg2 arg3 arg4] (f arg0 arg1 arg2 arg3 arg4))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5] (f arg0 arg1 arg2 arg3 arg4 arg5))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6] (f arg0 arg1 arg2 arg3 arg4 arg5 arg6))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7] (f arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8] (f arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9]
     (f arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10]
     (f arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11]
     (f arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12]
     (f arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13]
     (f arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14]
     (f arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15]
     (f arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16]
     (f arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16 arg17]
     (f arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16 arg17))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16 arg17 arg18]
     (f arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16 arg17 arg18))
   (invoke [_ arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16 arg17 arg18 arg19]
     (f arg0 arg1 arg2 arg3 arg4 arg5 arg6 arg7 arg8 arg9 arg10 arg11 arg12 arg13 arg14 arg15 arg16 arg17 arg18 arg19))
   (applyTo [_ args] (.applyTo ^clojure.lang.IFn f args))
   Autological
   (get-var [_] serial))

(defmacro def-autological-fn
  "This does not mirror defn. Just takes a name and some body. The body should
  evaluate to a function. Defines a var with supplied name, to which is assigned
  an AutologicalFunction containing a reference to the same var."
  [name body]
  `(do
     (declare ~name)
     (def ~name (->AutologicalFunction ~body (var ~name)))))

;; FRESSIAN HANDLERS

(def autological-reader
  (reify org.fressian.handlers.ReadHandler
    (read [_ reader tag component-count]
      (let [obj (.readObject reader)]
        (insist (var? obj))
        @obj))))

(def autological-writer
  (reify org.fressian.handlers.WriteHandler
    (write [_ writer f]
      (.writeTag writer "eva/autological" 1)
      (.writeObject writer (get-var f)))))
