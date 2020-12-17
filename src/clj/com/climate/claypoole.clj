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
  "Threadpool tools for Clojure. Claypoole provides parallel functions and
  macros that use threads from a pool and otherwise act like key builtins like
  future, pmap, for, and so on. See the file README.md for an introduction.

  A threadpool is just an ExecutorService with a fixed number of threads. In
  general, you can use your own ExecutorService in place of any threadpool, and
  you can treat a threadpool as you would any other ExecutorService."
  (:refer-clojure :exclude [future future-call pcalls pmap pvalues])
  (:require
    [clojure.core :as core]
    [com.climate.claypoole.impl :as impl])
  (:import
    [java.util.function Supplier]
    [com.climate.claypoole.impl
     PriorityThreadpool
     PriorityThreadpoolImpl]
    [java.util.concurrent
     Callable
     CancellationException
     ExecutorService
     Future
     CompletableFuture
     LinkedBlockingQueue
     ScheduledExecutorService]))


(def ^:dynamic *parallel*
  "A dynamic binding to disable parallelism. If you do
    (binding [*parallel* false]
      body)
  then the body will have no parallelism. Disabling parallelism this way is
  handy for testing."
  true)

(def ^:dynamic *default-pmap-buffer*
  "This is an advanced configuration option. You probably don't need to set
  this!

  When doing a pmap, Claypoole pushes input tasks into the threadpool. It
  normally tries to keep the threadpool full, plus it adds a buffer of size
  nthreads. If it can't find out the number of threads in the threadpool, it
  just tries to keep *default-pmap-buffer* tasks in the pool."
  200)

(defn ncpus
  "Get the number of available CPUs."
  []
  (.. Runtime getRuntime availableProcessors))

(defn thread-factory
  "Create a ThreadFactory with keyword options including thread daemon status
  :daemon, the thread name format :name (a string for format with one integer),
  and a thread priority :thread-priority.

  This is exposed as a public function because it's handy if you're
  instantiating your own ExecutorServices."
  [& {:keys [daemon thread-priority] pool-name :name
      :as args}]
  (->> args
       (apply concat)
       (apply impl/thread-factory)))

(defn threadpool
  "Make a threadpool. It should be shutdown when no longer needed.

  A threadpool is just an ExecutorService with a fixed number of threads. In
  general, you can use your own ExecutorService in place of any threadpool, and
  you can treat a threadpool as you would any other ExecutorService.

  This takes optional keyword arguments:
    :daemon, a boolean indicating whether the threads are daemon threads,
             which will automatically die when the JVM exits, defaults to
             true)
    :name, a string giving the pool name, which will be the prefix of each
             thread name, resulting in threads named \"name-0\",
             \"name-1\", etc. Defaults to \"claypoole-[pool-number]\".
    :thread-priority, an integer in [Thread/MIN_PRIORITY, Thread/MAX_PRIORITY].
             The effects of thread priority are system-dependent and should not
             be confused with Claypoole's priority threadpools that choose
             tasks based on a priority. For more info about Java thread
             priority see
             http://www.javamex.com/tutorials/threads/priority_what.shtml

  Note: Returns a ScheduledExecutorService rather than just an ExecutorService
  because it's the same thing with a few bonus features."
  ;; NOTE: The Clojure compiler doesn't seem to like the tests if we don't
  ;; fully expand this typename.
  ^java.util.concurrent.ScheduledExecutorService
  ;; NOTE: Although I'm repeating myself, I list all the threadpool-factory
  ;; arguments explicitly for API clarity.
  [n & {:keys [daemon thread-priority] pool-name :name
        :or {daemon true}}]
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
  ^com.climate.claypoole.impl.PriorityThreadpool
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

    (upmap (with-priority-fn pool (fn [x _] x))
           + [6 5 4] [1 2 3])

  will use pool to run tasks [(+ 6 1) (+ 5 2) (+ 4 3)]
  with priorities [6 5 4]."
  ^com.climate.claypoole.impl.PriorityThreadpool
  [^com.climate.claypoole.impl.PriorityThreadpool pool priority-fn]
  (impl/with-priority-fn pool priority-fn))

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
  ^java.util.concurrent.ExecutorService [^ExecutorService pool priority]
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
  expression, calling shutdown! on the threadpool to forcibly shut it down at
  the end.

  The threadpool initializer may be a threadpool. Alternately, it can be any
  threadpool argument accepted by pmap, e.g. a number, :builtin, or :serial, in
  which case it will create a threadpool just as pmap would.

  Be aware that any unfinished jobs at the end of the body will be killed!

  Examples:

      (with-shutdown! [pool (threadpool 6)]
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

(defn serial?
  "Check if we should run computations on this threadpool in serial."
  [pool]
  (or (not *parallel*) (= pool :serial)))

(defn future-call
  "Like clojure.core/future-call, but using a threadpool.

  The threadpool may be one of 3 things:
    1. An ExecutorService, e.g. one created by threadpool.
    2. The keyword :builtin. In this case, the future will use the built-in
       agent threadpool, the same threadpool used by an ordinary
       clojure.core/future.
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
    2. The keyword :builtin. In this case, the future will use the built-in
       agent threadpool, the same threadpool used by an ordinary
       clojure.core/future.
    3. The keyword :serial. In this case, the computation will be performed in
       serial. This may be helpful during profiling, for example.
  "
  [pool & body]
  `(future-call ~pool (^{:once true} fn ~'future-body [] ~@body)))

(defn completable-future-call
  "Like clojure.core/future-call, but using a threadpool, and returns a CompletableFuture.

  The threadpool may be one of 3 things:
  1. An ExecutorService, e.g. one created by threadpool.
  2. The keyword :builtin. In this case, the future will use the built-in
  agent threadpool, the same threadpool used by an ordinary
  clojure.core/future.
  3. The keyword :serial. In this case, the computation will be performed in
  serial. This may be helpful during profiling, for example.
  "
  [pool f]
  (impl/validate-future-pool pool)
  (cond
   ;; If requested, run the future in serial.
   (serial? pool) (CompletableFuture/completedFuture (f))
   ;; If requested, use the default threadpool.
   (= :builtin pool) (completable-future-call clojure.lang.Agent/soloExecutor f)
   :else
   (let [f* (impl/binding-conveyor-fn f)]
     (CompletableFuture/supplyAsync
      (reify Supplier
        (get [_]
          (f*))) pool))))

(defmacro completable-future
  "Like clojure.core/future, but using a threadpool and returns a CompletableFuture.

  The threadpool may be one of 3 things:
    1. An ExecutorService, e.g. one created by threadpool.
    2. The keyword :builtin. In this case, the future will use the built-in
       agent threadpool, the same threadpool used by an ordinary
       clojure.core/future.
    3. The keyword :serial. In this case, the computation will be performed in
       serial. This may be helpful during profiling, for example.
  "
  [pool & body]
  `(completable-future-call ~pool (^{:once true} fn ~'completable-future-body [] ~@body)))

(defn- buffer-blocking-seq
  "Make a lazy sequence that blocks when the map's (imaginary) buffer is full."
  [pool unordered-results]
  (let [buffer-size (if-let [pool-size (impl/get-pool-size pool)]
                      (* 2 pool-size)
                      *default-pmap-buffer*)]
    (concat (repeat buffer-size nil)
            unordered-results)))

(defn- pmap-core
  "Given functions to customize for pmap or upmap, do the hard work of pmap."
  [pool ordered? f arg-seqs]
  (let [[shutdown? pool] (impl/->threadpool pool)
        ;; Use map to handle the argument sequences.
        args (apply map vector (map impl/unchunk arg-seqs))
        ;; We set this to true to stop realizing more tasks.
        abort (atom false)
        ;; Set up queues of tasks and results
        [task-q tasks] (impl/queue-seq)
        [unordered-results-q unordered-results] (impl/queue-seq)
        ;; This is how we'll actually make things go.
        start-task (fn [i a]
                     ;; We can't directly make a future add itself to a
                     ;; queue. Instead, we use a promise for indirection.
                     (let [p (promise)]
                       (deliver p (future-call
                                    pool
                                    (with-meta
                                      ;; Try to run the task, but definitely
                                      ;; add the future to the queue.
                                      #(try
                                         (apply f a)
                                         (catch Throwable t
                                           ;; If we've had an exception, stop
                                           ;; making new tasks.
                                           (reset! abort true)
                                           ;; Re-throw that throwable!
                                           (throw t))
                                         (finally
                                           (impl/queue-seq-add!
                                             unordered-results-q @p)))
                                      ;; Add the args to the function's
                                      ;; metadata for prioritization.
                                      {:args a})))
                       @p))
        ;; Start all the tasks in a real future, so we don't block.
        driver (core/future
                 (try
                   (doseq [[i a _]
                           (map vector (range) args
                                ;; The driver thread reads from this sequence
                                ;; and ignores the result, just to get the side
                                ;; effect of blocking when the map's
                                ;; (imaginary) buffer is full.
                                (buffer-blocking-seq pool unordered-results))
                           :while (not @abort)]
                     (impl/queue-seq-add! task-q (start-task i a)))
                   (finally
                     (impl/queue-seq-end! task-q)
                     (when shutdown? (shutdown pool)))))
        result-seq (if ordered?
                     tasks
                     (map second (impl/lazy-co-read tasks unordered-results)))]
    ;; Read results as available.
    (concat
      (map impl/deref-fixing-exceptions result-seq)
      ;; Deref the read-future to get its exceptions, if it has any.
      (lazy-seq @driver))))

(defn- pmap-boilerplate
  "Do boilerplate pmap checks, then call the real pmap function."
  [pool ordered? f arg-seqs]
  (when (empty? arg-seqs)
    (throw (IllegalArgumentException.
             "pmap requires at least one sequence to map over")))
  (if (serial? pool)
    (doall (apply map f arg-seqs))
    (pmap-core pool ordered? f arg-seqs)))

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
    2. An integer. In this case, a threadpool will be created, and it will be
       destroyed when all the pmap tasks are complete.
    3. The keyword :builtin. In this case, pmap will use the Clojure built-in
       agent threadpool. For pmap, that's probably not what you want, as you'll
       likely create a thread per task.
    4. The keyword :serial. In this case, the computations will be performed in
       serial via (doall map). This may be helpful during profiling, for example.
  "
  [pool f & arg-seqs]
  (pmap-boilerplate pool true f arg-seqs))

(defn upmap
  "Like pmap, except that the return value is a sequence of results ordered by
  *completion time*, not by input order."
  [pool f & arg-seqs]
  (pmap-boilerplate pool false f arg-seqs))

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
      (pfor pool [i (range 1000) :let [result (complex-computation i)]] result)

  You can use the special binding :priority (which must be the last binding) to
  set the priorities of the tasks.
      (upfor (priority-threadpool 10) [i (range 1000)
                                       :priority (inc i)]
        (complex-computation i))
  "
  [pool bindings & body]
  (impl/pfor-internal pool bindings body `pmap))

(defmacro upfor
  "Like pfor, except the return value is a sequence of results ordered by
  *completion time*, not by input order."
  [pool bindings & body]
  (impl/pfor-internal pool bindings body `upmap))

(defmacro pdoseq
  "Like doseq, but in parallel. Unlike the streaming sequence functions (e.g.
  pmap), pdoseq blocks until all the work is done.

  Similar to pfor, only the body is done in parallel. For more details, see
  pfor."
  [pool bindings & body]
  `(dorun (upfor ~pool ~bindings (do ~@body))))

(defn prun!
  "Like run!, but in parallel. Unlike the streaming sequence functions (e.g.
  pmap), prun! blocks until all the work is done."
  [pool proc coll]
  (dorun (upmap pool proc coll)))
