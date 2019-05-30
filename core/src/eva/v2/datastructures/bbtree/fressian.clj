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

(ns eva.v2.datastructures.bbtree.fressian
  "The fressian tags common to all versions -- mostly Clojure Core stuff."
  (:require [eva.datastructures.utils.interval :as interval]
            [eva.datastructures.utils.fressian :as eva-fresh]
            [eva.v2.datastructures.bbtree.error :refer :all]
            [eva.v2.datastructures.bbtree.fressian.v0 :as v0]
            [eva.error :refer [error? insist]]
            [recide.core :refer [update-error]]
            [clojure.data.avl :as avl])
  (:import [eva ByteString]
           [clojure.lang IPersistentVector IPersistentList PersistentHashSet Var]
           [eva.datastructures.utils.comparators Comparator]
           [eva.datastructures.utils.interval Interval]
           [eva.datastructures.utils.fressian Autological AutologicalFunction]))

(def vector-reader
  (reify org.fressian.handlers.ReadHandler
    (read [_ reader tag component-count]
      (let [obj (.readObject ^org.fressian.Reader reader)]
        (try (vec obj)
             (catch Exception e
               (if (error? e :fressian.unreadable/*)
                 (throw (update-error e :handler-chain conj :vector))
                 (raise-fressian-read-err :vector "Call to 'vec' failed." {:handler-chain [:vector]} e))))))))

(def vector-writer
  (reify org.fressian.handlers.WriteHandler
    (write [_  writer v]
      (.writeTag ^org.fressian.Writer writer "clj/vector" 1)
      (.writeList writer v))))

(def list-reader
  (reify org.fressian.handlers.ReadHandler
    (read [_ reader tag component-count]
      (try (apply list (.readObject reader))
           (catch Exception e
             (if (error? e :fressian.unreadable/*)
               (throw (update-error e :handler-chain conj :list))
               (raise-fressian-read-err :list "" {:handler-chain [:list]} e)))))))

(def list-writer
  (reify org.fressian.handlers.WriteHandler
    (write [_ writer l]
      (.writeTag writer "clj/list" 1)
      (.writeList writer l))))

(def set-reader
  (reify org.fressian.handlers.ReadHandler
    (read [_ reader tag component-count]
      (try (into #{} (.readObject reader))
           (catch Exception e
             (if (error? e :fressian.unreadable/*)
               (throw (update-error e :handler-chain conj :set))
               (raise-fressian-read-err :set "" {:handler-chain [:set]} e)))))))

(def set-writer
  (reify org.fressian.handlers.WriteHandler
    (write [_ writer hash-set]
      (.writeTag writer "clj/set" 1)
      (.writeList writer hash-set))))

(def var-reader
  (reify org.fressian.handlers.ReadHandler
    (read [_ reader tag component-count]
      (let [s (.readObject reader)]
        (if-let [found-var (find-var s)]
          found-var
          (raise-fressian-read-err :var (format "'%s'" (pr-str s)) {:handler-chain [:var]}))))))

(def var-writer
  (reify org.fressian.handlers.WriteHandler
    (write [_ writer v]
      (insist (var? v))
      (let [^clojure.lang.Var v v
            var-sym (fn var-sym [x]
                      (when (var? x)
                        (symbol (str (ns-name (:ns (meta x))))
                                (str (:name (meta x))))))
            vs (var-sym v)]
        (insist (symbol? vs))
        (.writeTag writer "clj/var" 1)
        (.writeObject writer vs)))))

(def byte-string-reader
  (reify org.fressian.handlers.ReadHandler
    (read [_ reader tag component-count]
      (try (ByteString/wrapping ^"[B" (.readObject reader))
           (catch Exception e
             (if (error? e :fressian.unreadable/*)
               (throw (update-error e :handler-chain conj :byte-string))
               (raise-fressian-read-err :byte-string "" {:handler-chain [:byte-string]} e)))))))

(def byte-string-writer
  (reify org.fressian.handlers.WriteHandler
    (write [_ writer bstr]
      (.writeTag writer "eva/byte-string" 1)
      (.writeObject writer (.toByteArray ^ByteString bstr)))))

(def all-writers
  (merge v0/all-writers
         {clojure.lang.IPersistentVector {"clj/vector" vector-writer}
          clojure.lang.ISeq {"clj/list" list-writer}
          clojure.lang.PersistentHashSet {"clj/set" set-writer}
          clojure.lang.Var {"clj/var" var-writer}
          ByteString {"eva/byte-string" byte-string-writer}
          Comparator {"eva/autological" eva-fresh/autological-writer}
          Interval {"eva/interval" interval/interval-writer}
          AutologicalFunction {"eva/autological" eva-fresh/autological-writer}}))

(def all-readers
  (merge v0/all-readers
         {"clj/vector" vector-reader
          "clj/list" list-reader
          "cloj/seq" list-reader
          "clj/set" set-reader
          "clj/var" var-reader
          "eva/byte-string" byte-string-reader
          "eva/autological" eva-fresh/autological-reader
          "eva/interval" interval/interval-reader}))
