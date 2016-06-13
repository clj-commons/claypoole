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
  "Lazy threadpool tools for Clojure.

  This namespace provides lazy versions of the parallel functions from
  com.climate.claypoole. They will only run the outputs you realize, plus a few
  more (a buffer), just like core pmap. The buffer is used to help keep the
  threadpool busy.

  Each parallel function also comes with a -buffer variant that allows you to
  specify the buffer size. The non -buffer forms use the threadpool size as
  their buffer size.

  To use the threadpool most efficiently with these lazy functions, prefer the
  unordered versions (e.g. upmap), since the ordered ones may starve the
  threadpool of work. For instance, this pmap will take 6 milliseconds to run:
    (lazy/pmap 2 #(Thread/sleep %) [4 3 2 1])
  That's because it will not realize the 2 task until the 4 task is complete,
  so one thread in the pool will sit idle for 1 millisecond."
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

(defn pmap-buffer
  "A lazy pmap where the work happens in a threadpool, just like core pmap, but
  using Claypoole futures.

  Unlike core pmap, it doesn't assume the buffer size is nprocessors + 2;
  instead, you must specify how many tasks ahead will be run in the
  background.

  If you
    (doall (take 2 (pmap-buffer pool 10 inc (range 1000))))
  then 12 inputs and outputs will be realized--the 2 you asked for, plus the 10
  that are run in the buffer to keep the threadpool busy."
  [pool buffer-size f & colls]
  (if (cp/serial? pool)
    (apply map f colls)
    (let [[shutdown? pool] (impl/->threadpool pool)]
      (->> colls
           ;; make sure we're not chunking
           (map impl/unchunk)
           ;; use map to take care of argument alignment
           (apply map vector)
           ;; make futures
           (map (fn [a] (cp/future-call pool
                                        ;; Use with-meta for priority
                                        ;; threadpools
                                        (with-meta #(apply f a)
                                                   {:args a}))))
           ;; force buffer-size futures to start work in the pool
           (forceahead (or buffer-size (impl/get-pool-size pool) 0))
           ;; read the results from the futures
           (map impl/deref-fixing-exceptions)
           (impl/seq-open #(when shutdown? (cp/shutdown pool)))))))

(defn pmap
  "A lazy pmap where the work happens in a threadpool, just like core pmap, but
  using Claypoole futures in the given threadpool.

  Unlike core pmap, it doesn't assume the buffer size is nprocessors + 2;
  instead, it tries to fill the pool.

  If you
    (doall (take 2 (pmap 10 inc (range 1000))))
  then 12 inputs and outputs will be realized--the 2 you asked for, plus the 10
  that are run in the buffer to keep the threadpool busy."
  [pool f & colls]
  (apply pmap-buffer pool nil f colls))

(defn upmap-buffer
  "Like pmap-buffer, but with results returned in the order they completed.

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
           ;; make sure we're not chunking
           (map impl/unchunk)
           ;; use map to take care of argument alignment
           (apply map vector)
           ;; make futures
           (map run-one)
           ;; force buffer-size futures to start work in the pool
           (forceahead buffer-size)
           ;; read the results from the futures in the queue
           (map (fn [_] (impl/deref-fixing-exceptions (.take result-q))))
           (impl/seq-open #(if shutdown? (cp/shutdown pool)))))))

(defn upmap
  "Like pmap, but with results returned in the order they completed.

  Note that unlike core pmap, it doesn't assume the buffer size is nprocessors
  + 2; instead, it tries to fill the pool."
  [pool f & colls]
  (apply upmap-buffer pool (impl/get-pool-size pool) f colls))

(defn pcalls
  "Like clojure.core.pcalls, except it's lazy and it takes a threadpool. For
  more detail on its parallelism and on its threadpool argument, see pmap."
  [pool & fs]
  (pmap pool #(%) fs))

(defn pcalls-buffer
  "Like clojure.core.pcalls, except it's lazy and it takes a threadpool and a
  buffer size. For more detail on these arguments, see pmap-buffer."
  [pool buffer & fs]
  (pmap-buffer pool buffer #(%) fs))

(defn upcalls
  "Like clojure.core.pcalls, except it's lazy, it takes a threadpool, and it
  returns results ordered by completion time. For more detail on its
  parallelism and on its threadpool argument, see upmap."
  [pool & fs]
  (upmap pool #(%) fs))

(defn upcalls-buffer
  "Like clojure.core.pcalls, except it's lazy, it takes a threadpool and a
  buffer size, and it returns results ordered by completion time. For more
  detail on its parallelism and on its arguments, see upmap-buffer."
  [pool buffer & fs]
  (upmap-buffer pool buffer #(%) fs))

(defmacro pvalues
  "Like clojure.core.pvalues, except it's lazy and it takes a threadpool. For
  more detail on its parallelism and on its threadpool argument, see pmap."
  [pool & exprs]
  `(pcalls ~pool ~@(for [e exprs] `(fn [] ~e))))

(defmacro pvalues-buffer
  "Like clojure.core.pvalues, except it's lazy and it takes a threadpool and a
  buffer size. For more detail on its parallelism and on its arguments, see
  pmap-buffer."
  [pool buffer & exprs]
  `(pcalls-buffer ~pool ~buffer ~@(for [e exprs] `(fn [] ~e))))

(defmacro upvalues
  "Like clojure.core.pvalues, except it's lazy, it takes a threadpool, and it
  returns results ordered by completion time. For more detail on its
  parallelism and on its threadpool argument, see upmap."
  [pool & exprs]
  `(upcalls ~pool ~@(for [e exprs] `(fn [] ~e))))

(defmacro upvalues-buffer
  "Like clojure.core.pvalues, except it's lazy, it takes a threadpool and a
  buffer size, and it returns results ordered by completion time. For more
  detail on its parallelism and on its arguments, see upmap-buffer."
  [pool buffer & exprs]
  `(upcalls-buffer ~pool ~buffer ~@(for [e exprs] `(fn [] ~e))))

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

(defmacro pfor-buffer
  "Like pfor, but it takes a buffer size; see pmap-buffer for information about
  this argument."
  [pool buffer bindings & body]
  ;; Instead of pmap, we'll use an inline function with the buffer thrown in
  ;; there. It's hacky, because it relies on exactly how pfor-internal expands,
  ;; but it works.
  (let [pm `(fn [p# f# & cs#] (apply pmap-buffer p# ~buffer f# cs#))]
    (impl/pfor-internal pool bindings body pm)))

(defmacro upfor
  "Like pfor, except the return value is a sequence of results ordered by
  *completion time*, not by input order."
  [pool bindings & body]
  (impl/pfor-internal pool bindings body `upmap))

(defmacro upfor-buffer
  "Like upfor, but it takes a buffer size; see pmap-buffer for information
  about this argument."
  [pool buffer bindings & body]
  ;; Instead of pmap, we'll use an inline function with the buffer thrown in
  ;; there. It's hacky, because it relies on exactly how pfor-internal expands,
  ;; but it works.
  (let [upm `(fn [p# f# & cs#] (apply upmap-buffer p# ~buffer f# cs#))]
    (impl/pfor-internal pool bindings body upm)))

(defmacro pdoseq
  "Like doseq, but in parallel. Unlike the streaming sequence functions (e.g.
  pmap), pdoseq blocks until all the work is done.

  Similar to pfor, only the body is done in parallel. For more details, see
  pfor."
  [pool bindings & body]
  ;; There's no sensible lazy version of this, so just use base Claypoole's
  ;; implementation.
  `(cp/pdoseq ~pool ~bindings ~@body))

(defn prun!
  "Like run!, but in parallel. Unlike the streaming sequence functions (e.g.
  pmap), prun! blocks until all the work is done."
  [pool proc coll]
  ;; There's no sensible lazy version of this, so just use base Claypoole's
  ;; implementation.
  (cp/prun! pool proc coll))
