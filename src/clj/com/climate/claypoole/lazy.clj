;; The Climate Corporation licenses this file to you under under the Apache
;; License, Version 2.0 (the "License"); you may not use this file except in
;; compliance with the License.  You may obtain a copy of the License at
;;
;;   http://www.apache.org/licenses/LICENSE-2.0
;;
;; See the NOTICE file distributed with this work for additional information
;; regarding copyright ownership.  Unless required by applicable law or agreed
;; to in writing, software distributed under the License is distributed on an
;; "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
;; or implied.  See the License for the specific language governing permissions
;; and limitations under the License.

(ns com.climate.claypoole.lazy
  (:refer-clojure :exclude [future future-call pcalls pmap pvalues])
  (:require [com.climate.claypoole :as cp]
            [com.climate.claypoole.impl :as impl])
  (:import [java.util.concurrent LinkedBlockingQueue]))


(defn- forceahead
  "A sequence of s with the next buffer-size elements of it forced."
  [buffer-size s]
  (map (fn [x _] x)
       s
       (concat (drop buffer-size s) (repeat nil))))

(defn- pool-closer
  "Given a sequence, concat onto its end a lazy sequence that will shutdown a
  pool when forced."
  [shutdown? pool s]
  (concat s
          ;; Shutdown the pool if needed.
          (lazy-seq
            (do
              (when shutdown? (cp/shutdown pool))
              nil))))

(defn pmap-manual
  "A lazy pmap where the work happens in a threadpool, just like core pmap, but
  using claypoole futures.

  Unlike core pmap, it doesn't assume the buffer size is nprocessors + 2;
  instead, you must specify how many tasks ahead will be run in the
  background."
  [pool buffer-size f & colls]
  (if (cp/serial? pool)
    (apply map f colls)
    (let [[shutdown? pool] (impl/->threadpool pool)]
      (->> colls
           ;; use map to take care of argument alignment
           (apply map vector)
           ;; make sure we're not chunking
           impl/unchunk
           ;; make futures
           (map (fn [a] (cp/future-call pool
                                        ;; Use with-meta for priority
                                        ;; threadpools
                                        (with-meta #(apply f a)
                                                   {:args a}))))
           ;; force buffer-size futures to start work in the pool
           (forceahead (or buffer-size (impl/get-pool-size pool) 0))
           ;; read the results from the futures
           (map deref)
           (pool-closer shutdown? pool)))))

(defn pmap
  "A lazy pmap where the work happens in a threadpool, just like core pmap, but
  using claypoole futures.

  Unlike core pmap, it doesn't assume the buffer size is nprocessors + 2;
  instead, it tries to fill the pool."
  [pool f & colls]
  (apply pmap-manual pool nil f colls))

(defn upmap-manual
  "Like pmap-manual, but with results returned in the order they completed.

  Note that unlike core pmap, it doesn't assume the buffer size is nprocessors
  + 2; instead, you must specify how many tasks ahead will be run in the
  background."
  [pool buffer-size f & colls]
  (if (cp/serial? pool)
    (apply map f colls)
    (let [[shutdown? pool] (impl/->threadpool pool)
          buffer-size (or buffer-size (impl/get-pool-size pool) 10)
          result-q (LinkedBlockingQueue. (int buffer-size))
          run-one (fn [a]
                    (let [p (promise)]
                      @(deliver p
                                (cp/future-call
                                  pool
                                  ;; Use with-meta for priority threadpools
                                  (with-meta #(try (apply f a)
                                                   (finally (.put result-q @p)))
                                             {:args a})))))]
      (->> colls
           ;; use map to take care of argument alignment
           (apply map vector)
           ;; make sure we're not chunking
           impl/unchunk
           ;; make futures
           (map run-one)
           ;; force buffer-size futures to start work in the pool
           (forceahead buffer-size)
           ;; read the results from the futures in the queue
           (map (fn [_] (deref (.take result-q))))
           (pool-closer shutdown? pool)))))

(defn upmap
  "Like pmap, but with results returned in the order they completed.

  Note that unlike core pmap, it doesn't assume the buffer size is nprocessors
  + 2; instead, it tries to fill the pool."
  [pool f & colls]
  (apply upmap-manual pool (impl/get-pool-size pool) f colls))

;; TODO move the following definitions to a macro? But a macro-defining-macro
;; is a bit ugly

(defn pcalls
  "Like clojure.core.pcalls, except it takes a threadpool. For more detail on
  its parallelism and on its threadpool argument, see pmap."
  [pool & fs]
  (pmap pool #(%) fs))

(defn pcalls-manual
  [pool buffer & fs]
  (pmap-manual pool buffer #(%) fs))

(defn upcalls
  "Like clojure.core.pcalls, except it takes a threadpool and returns results
  ordered by completion time. For more detail on its parallelism and on its
  threadpool argument, see upmap."
  [pool & fs]
  (upmap pool #(%) fs))

(defn upcalls-manual
  [pool buffer & fs]
  (upmap-manual pool buffer #(%) fs))

(defmacro pvalues
  "Like clojure.core.pvalues, except it takes a threadpool. For more detail on
  its parallelism and on its threadpool argument, see pmap."
  [pool & exprs]
  `(pcalls ~pool ~@(for [e exprs] `(fn [] ~e))))

(defmacro pvalues-manual
  [pool buffer & exprs]
  `(pcalls-manual ~pool ~buffer ~@(for [e exprs] `(fn [] ~e))))

(defmacro upvalues
  "Like clojure.core.pvalues, except it takes a threadpool and returns results
  ordered by completion time. For more detail on its parallelism and on its
  threadpool argument, see upmap."
  [pool & exprs]
  `(upcalls ~pool ~@(for [e exprs] `(fn [] ~e))))

(defmacro upvalues-manual
  [pool buffer & exprs]
  `(upcalls-manual ~pool ~buffer ~@(for [e exprs] `(fn [] ~e))))

(defmacro pfor
  "A parallel version of for. It is like for, except it takes a threadpool and
  is parallel. For more detail on its parallelism and on its threadpool
  argument, see pmap.

  Note that while the body is executed in parallel, the bindings are executed
  in serial, so while this will call complex-computation in parallel:
      (pfor pool [i (range 1000)] (complex-computation i))
  this will not have useful parallelism:
      (pfor pool [i (range 1000) :let [result (complex-computation i)]] result)

  You can use the special binding :priority (which must be the last binding) to
  set the priorities of the tasks.
      (upfor (priority-threadpool 10) [i (range 1000)
                                       :priority (inc i)]
        (complex-computation i))
  "
  [pool bindings & body]
  (impl/pfor-internal pool bindings body `pmap))

(defmacro pfor-manual
  [pool buffer bindings & body]
  ;; Instead of pmap, we'll use an inline function with the buffer thrown in
  ;; there. It's hacky, because it relies on exactly how pfor-internal expands,
  ;; but it works.
  (let [pm `(fn [p# f# & cs#] (apply pmap-manual p# ~buffer f# cs#))]
    (impl/pfor-internal pool bindings body pm)))

(defmacro upfor
  "Like pfor, except the return value is a sequence of results ordered by
  *completion time*, not by input order."
  [pool bindings & body]
  (impl/pfor-internal pool bindings body `upmap))

(defmacro upfor-manual
  [pool buffer bindings & body]
  ;; Instead of pmap, we'll use an inline function with the buffer thrown in
  ;; there. It's hacky, because it relies on exactly how pfor-internal expands,
  ;; but it works.
  (let [upm `(fn [p# f# & cs#] (apply upmap-manual p# ~buffer f# cs#))]
    (impl/pfor-internal pool bindings body upm)))
