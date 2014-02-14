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

(ns com.climate.claypoole
  "Threadpool tools for Clojure."
  (:refer-clojure :exclude [future future-call pcalls pmap pvalues])
  (:require
    [clojure.core :as core]
    [com.climate.claypoole.impl :as impl])
  (:import
    [java.util.concurrent
     Callable
     ExecutorService
     Future
     LinkedBlockingQueue]))


(def ^:dynamic *parallel*
  "A dynamic binding to disable parallelism. If you do
    (binding [*parallel* false]
      body)
  then the body will have no parallelism. Disabling parallelism this way is
  handy for testing."
  true)

(defn ncpus
  "Get the number of available CPUs."
  []
  (.. Runtime getRuntime availableProcessors))

(defn threadpool
  "Make a threadpool. It should be shutdown when no longer needed.

  This takes optional keyword arguments:
    :daemon, a boolean indicating whether the threads are daemon threads,
             which will automatically die when the JVM exits, defaults to
             false)
    :name, a string giving the pool name, which will be the prefix of each
             thread name, resulting in threads named \"name-0\",
             \"name-1\", etc. Defaults to \"claypoole-[pool-number]\".
    :thread-priority, an integer in [Thread/MIN_PRIORITY, Thread/MAX_PRIORITY]
             giving the priority of each thread, defaults to the priority of
             the current thread"
  ([] (threadpool (+ 2 (ncpus))))
  ([n & {:keys [daemon thread-priority] pool-name :name}]
   (impl/threadpool n
                    :daemon daemon
                    :name pool-name
                    :thread-priority thread-priority)))

(defn threadpool?
  "Returns true iff the argument is a threadpool."
  [pool]
  (instance? ExecutorService pool))

(defn shutdown
  "Syntactic sugar to stop a pool cleanly. This will stop the pool from
  accepting any new requests."
  [^ExecutorService pool]
  (when (not (= pool clojure.lang.Agent/soloExecutor))
    (.shutdown pool)))

(defn shutdown!
  "Syntactic sugar to forcibly shutdown a pool. This will kill any running
  threads in the pool!"
  [^ExecutorService pool]
  (when (not (= pool clojure.lang.Agent/soloExecutor))
    (.shutdownNow pool)))

(defn shutdown?
  "Syntactic sugar to test if a pool is shutdown."
  [^ExecutorService pool]
  (.isShutdown pool))

(defmacro with-shutdown!
  "Lets a threadpool from an initializer, then evaluates body in a try
  expression, forcibly shutting down the threadpool at the end.

  The threadpool initializer may be a threadpool. Alternately, it can be any
  threadpool argument accepted by pmap, e.g. a number, :builtin, or :serial, in
  which case it will create a threadpool just as pmap would.

  Be aware that any unfinished jobs at the end of the body will be killed!

  Examples:

      (with-shutdown! [pool (theadpool 6)]
        (doall (pmap pool identity (range 1000))))
      (with-shutdown! [pool 6]
        (doall (pmap pool identity (range 1000))))

  Bad example:

      (with-shutdown! [pool 6]
        ;; Some of these tasks may be killed!
        (pmap pool identity (range 1000)))
  "
  [[pool-sym pool-init] & body]
  `(let [pool-init# ~pool-init]
     (let [[_# ~pool-sym] (impl/->threadpool pool-init#)]
       (try
         ~@body
         (finally
           (when (threadpool? ~pool-sym)
             (shutdown! ~pool-sym)))))))

(defn- serial?
  "Check if we should run this computation in serial."
  [pool]
  (or (not *parallel*) (= pool :serial)))

(defn future-call
  "Like clojure.core/future-call, but using a threadpool.

  The threadpool may be one of 3 things:
    1. An ExecutorService, e.g. one created by threadpool.
    2. The keyword :default. In this case, the future will use the same
       threadpool as an ordinary clojure.core/future.
    3. The keyword :serial. In this case, the computation will be performed in
       serial. This may be helpful during profiling, for example.
  "
  [pool f]
  (impl/validate-future-pool pool)
  (cond
    ;; If requested, run the future in serial.
    (serial? pool) (impl/dummy-future-call f)
    ;; If requested, use the default threadpool.
    (= :builtin pool) (future-call clojure.lang.Agent/soloExecutor f)
    :else (let [;; We have to get the casts right, or the compiler will choose
                ;; the (.submit ^Runnable) call, which returns nil. We don't
                ;; want that!
                ^ExecutorService pool* pool
                ^Callable f* (impl/binding-conveyor-fn f)
                fut (.submit pool* f*)]
            (reify
              clojure.lang.IDeref
              (deref [_] (impl/deref-future fut))
              clojure.lang.IBlockingDeref
              (deref
                [_ timeout-ms timeout-val]
                (impl/deref-future fut timeout-ms timeout-val))
              clojure.lang.IPending
              (isRealized [_] (.isDone fut))
              Future
              (get [_] (.get fut))
              (get [_ timeout unit] (.get fut timeout unit))
              (isCancelled [_] (.isCancelled fut))
              (isDone [_] (.isDone fut))
              (cancel [_ interrupt?] (.cancel fut interrupt?))))))

(defmacro future
  "Like clojure.core/future, but using a threadpool.

  The threadpool may be one of 3 things:
    1. An ExecutorService, e.g. one created by threadpool.
    2. The keyword :default. In this case, the future will use the same
       threadpool as an ordinary clojure.core/future.
    3. The keyword :serial. In this case, the computation will be performed in
       serial. This may be helpful during profiling, for example.
  "
  [pool & body]
  `(future-call ~pool (^{:once true} fn [] ~@body)))

(defn pmap
  "Like clojure.core.pmap, except:
    1. It is eager, returning an ordered sequence of completed results as they
       are available.
    2. It uses a threadpool to control the desired level of parallelism.

  A word of caution: pmap will consume the entire input sequence and produce as
  much output as possible--if you pmap over (range) you'll get an Out of Memory
  Exception! Use this when you definitely want the work to be done.

  The threadpool may be one of 4 things:
    1. An ExecutorService, e.g. one created by threadpool.
    2. An integer. In case case, a threadpool will be created and destroyed
       at the end of the function.
    3. The keyword :cpu. In this case, a threadpool will be created with
       ncpus + 2 threads, same parallelism as clojure.core.pmap, and it will be
       destroyed at the end of the function.
    4. The keyword :serial. In this case, the computations will be performed in
       serial via (doall map). This may be helpful during profiling, for example.
  "
  [pool f & arg-seqs]
  (if (serial? pool)
    (doall (apply map f arg-seqs))
    (let [[shutdown? pool] (impl/->threadpool pool)
          args (apply map vector (map impl/unchunk arg-seqs))
          futures (map (fn [a] (future pool (apply f a))) args)
          ;; Start eagerly parallel processing.
          read-future (core/future
                        (try
                          (dorun futures)
                          (finally (when shutdown? (shutdown pool)))))]
      ;; Read results as available.
      (concat (map deref futures)
              ;; Deref the reading future to get its exceptions, if it had any.
              (lazy-seq (deref read-future))))))

(defn upmap
  "Like pmap, except the return value is a sequence of results ordered by
  *completion time*, not by input order."
  [pool f & arg-seqs]
  (if (serial? pool)
    (doall (apply map f arg-seqs))
    (let [[shutdown? pool] (impl/->threadpool pool)
          args (apply map vector (map impl/unchunk arg-seqs))
          q (LinkedBlockingQueue.)
          ;; Start eagerly parallel processing.
          read-future (core/future
                        (try
                          (doseq [a args
                                  :let [p (promise)]]
                            (deliver p (future pool (try
                                                      (apply f a)
                                                      (finally (.add q @p))))))
                          (finally (when shutdown? (shutdown pool)))))]
      ;; Read results as available.
      (concat (for [_ args] (-> q .take deref))
              ;; Deref the reading future to get its exceptions, if it had any.
              (lazy-seq (deref read-future))))))

(defn pcalls
  "Like clojure.core.pcalls, except it takes a threadpool. For more detail on
  its parallelism and on its threadpool argument, see pmap."
  [pool & fs]
  (pmap pool #(%) fs))

(defn upcalls
  "Like clojure.core.pcalls, except it takes a threadpool and returns results
  ordered by completion time. For more detail on its parallelism and on its
  threadpool argument, see upmap."
  [pool & fs]
  (upmap pool #(%) fs))

(defmacro pvalues
  "Like clojure.core.pvalues, except it takes a threadpool. For more detail on
  its parallelism and on its threadpool argument, see pmap."
  [pool & exprs]
  `(pcalls ~pool ~@(for [e exprs] `(fn [] ~e))))

(defmacro upvalues
  "Like clojure.core.pvalues, except it takes a threadpool and returns results
  ordered by completion time. For more detail on its parallelism and on its
  threadpool argument, see upmap."
  [pool & exprs]
  `(upcalls ~pool ~@(for [e exprs] `(fn [] ~e))))

(defmacro pfor
  "A parallel version of for. It is like for, except it takes a threadpool and
  is parallel. For more detail on its parallelism and on its threadpool
  argument, see pmap.

  Note that while the body is executed in parallel, the bindings are executed
  in serial, so while this will call complex-computation in parallel:
      (pfor pool [i (range 1000)] (complex-computation i))
  this will not have useful parallelism:
      (pfor pool [i (range 1000) :let [result (complex-computation i)] result)
  "
  [pool bindings & body]
  `(apply pcalls ~pool (for ~bindings (fn [] ~@body))))

(defmacro upfor
  "An unordered, parallel version of for. It is like for, except it takes a
  threadpool, is parallel, and returns its results ordered by completion time.
  For more detail on its parallelism and on its threadpool argument, see
  upmap.

  Note that while the body is executed in parallel, the bindings are executed
  in serial, so while this will call complex-computation in parallel:
      (upfor pool [i (range 1000)]
        (complex-computation i))
  this will not have useful parallelism:
      (upfor pool [i (range 1000)
                   :let [result (complex-computation i)]]
        result)
  "
  [pool bindings & body]
  `(apply upcalls ~pool (for ~bindings (fn [] ~@body))))
