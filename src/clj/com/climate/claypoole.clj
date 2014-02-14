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
  "Threadpool tools for Clojure.

  A threadpool is just an ExecutorService with a fixed number of threads. In
  general, you can use your own ExecutorService in place of any threadpool, and
  you can treat a threadpool as you would any other ExecutorService."
  (:refer-clojure :exclude [future future-call pcalls pmap pvalues])
  (:require
    [clojure.core :as core]
    [com.climate.claypoole.impl :as impl])
  (:import
    [com.climate.claypoole.impl
     PriorityThreadpool
     PriorityThreadpoolImpl]
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

  A threadpool is just an ExecutorService with a fixed number of threads. In
  general, you can use your own ExecutorService in place of any threadpool, and
  you can treat a threadpool as you would any other ExecutorService.

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
  ;; NOTE: Although I'm repeating myself, I list all the threadpool-factory
  ;; arguments explicitly for API clarity.
  [n & {:keys [daemon thread-priority] pool-name :name}]
  (impl/threadpool n
                   :daemon daemon
                   :name pool-name
                   :thread-priority thread-priority))

(defn priority-threadpool
  "Make a threadpool that chooses tasks based on their priorities.

  Assign priorities to tasks by wrapping the pool with with-priority or
  with-priority-fn. You can also set a default priority with keyword argument
  :default-priority.

  Otherwise, this uses the same keyword arguments as threadpool, and functions
  just like any other ExecutorService."
  ^PriorityThreadpool
  [n & {:keys [default-priority] :as args
        :or {default-priority 0}}]
  (PriorityThreadpool.
    (PriorityThreadpoolImpl. n
                             ;; Use our thread factory options.
                             (impl/apply-map impl/thread-factory args)
                             default-priority)
    (constantly default-priority)))

(defn with-priority-fn
  "Make a priority-threadpool wrapper that uses a given priority function.

  The priority function is applied to a pmap'd function's arguments. e.g.

    (upmap (with-priority-fn p first) + [6 5 4] [1 2 3])

  will use pool p to run tasks [(+ 6 1) (+ 5 2) (+ 4 3)]
  with priorities [6 5 4]."
  ^PriorityThreadpool [^PriorityThreadpool pool priority-fn]
  (let [^PriorityThreadpoolImpl pool* (.pool pool)]
    (PriorityThreadpool. pool* priority-fn)))

(defn with-priority
  "Make a priority-threadpool wrapper with a given fixed priority.

  All tasks run with this pool wrapper will have the given priority. e.g.

    (def t1 (future (with-priority p 1) 1))
    (def t2 (future (with-priority p 2) 2))
    (def t3 (future (with-priority p 3) 3))

  will use pool p to run these tasks with priorities 1, 2, and 3 respectively.

  If you nest priorities, the outermost one \"wins\", so this task will be run
  at priority 3:

    (def wp (with-priority p 1))
    (def t1 (future (with-priority (with-priority wp 2) 3) :result))
  "
  ^ExecutorService [^ExecutorService pool priority]
  (with-priority-fn pool (constantly priority)))

(defn threadpool?
  "Returns true iff the argument is a threadpool."
  [pool]
  (instance? ExecutorService pool))

(defn priority-threadpool?
  "Returns true iff the argument is a priority-threadpool."
  [pool]
  (instance? PriorityThreadpool pool))

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
      (with-shutdown! [pool1 6
                       pool2 :serial]
        (doall (pmap pool1 identity (range 1000))))

  Bad example:

      (with-shutdown! [pool 6]
        ;; Some of these tasks may be killed!
        (pmap pool identity (range 1000)))
  "
  [pool-syms-and-inits & body]
  (when-not (even? (count pool-syms-and-inits))
    (throw (IllegalArgumentException.
             "with-shutdown! requires an even number of binding forms")))
  (if (empty? pool-syms-and-inits)
    `(do ~@body)
    (let [[pool-sym pool-init & more] pool-syms-and-inits]
      `(let [pool-init# ~pool-init
             [_# ~pool-sym] (impl/->threadpool pool-init#)]
         (try
           (with-shutdown! ~more ~@body)
           (finally
             (when (threadpool? ~pool-sym)
               (shutdown! ~pool-sym))))))))

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
            ;; Make an object just like Clojure futures.
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
  (when (empty? arg-seqs)
    (throw (IllegalArgumentException.
             "pmap requires at least one sequence to map over")))
  (if (serial? pool)
    (doall (apply map f arg-seqs))
    (let [[shutdown? pool] (impl/->threadpool pool)
          args (apply map vector (map impl/unchunk arg-seqs))
          futures (for [a args]
                    (future-call pool
                                 (with-meta #(apply f a)
                                            {:args a})))
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
  (when (empty? arg-seqs)
    (throw (IllegalArgumentException.
             "upmap requires at least one sequence to map over")))
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
                            ;; We can't directly make a future add itself to a
                            ;; queue. Instead, we use a promise for
                            ;; indirection.
                            (deliver p (future-call pool
                                                    (with-meta
                                                      #(try
                                                         (apply f a)
                                                         (finally (.add q @p)))
                                                      {:args a}))))
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

(defn- pfor-internal
  "Do the messy parsing of the :priority from the for bindings."
  [pool bindings body pmap-fn-sym]
  (when (vector? pool)
    (throw (IllegalArgumentException.
             (str "Got a vector instead of a pool--"
                  "did you forget to use a threadpool?"))))
  (if-not (= :priority (first (take-last 2 bindings)))
    ;; If there's no priority, everything is simple.
    `(~pmap-fn-sym ~pool #(%) (for ~bindings (fn [] ~@body)))
    ;; If there's a priority, God help us--let's pull that thing out.
    (let [bindings* (vec (drop-last 2 bindings))
          priority-value (last bindings)]
      `(let [pool# (with-priority-fn ~pool second)
             [fns# priorities#] (apply map vector
                                       (for ~bindings*
                                         [(fn [priority#] ~@body)
                                          ~priority-value]))]
         (~pmap-fn-sym pool# #(%1 %2) fns# priorities#)))))

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
  (pfor-internal pool bindings body `pmap))

(defmacro upfor
  "Like pfor, except the return value is a sequence of results ordered by
  *completion time*, not by input order."
  [pool bindings & body]
  (pfor-internal pool bindings body `upmap))
