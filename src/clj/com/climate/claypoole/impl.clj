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

(ns com.climate.claypoole.impl
  "Implementation helper functions for Claypoole."
  (:require
    [clojure.core :as core])
  (:import
    [clojure.lang
     IFn]
    [com.climate.claypoole.impl
     Prioritized
     PriorityThreadpoolImpl]
    [java.util
     Collection
     List]
    [java.util.concurrent
     ExecutionException
     Executors
     ExecutorService
     Future
     ThreadFactory
     TimeoutException
     TimeUnit]))


(defn binding-conveyor-fn
  "Like clojure.core/binding-conveyor-fn for resetting bindings to run a
  function in another thread."
  [f]
  (let [frame (clojure.lang.Var/cloneThreadBindingFrame)]
    (with-meta
      (fn []
        (clojure.lang.Var/resetThreadBindingFrame frame)
        (f))
      (meta f))))

(defn deref-future
  "Like clojure.core/deref-future."
  ([^Future fut]
   (.get fut))
  ([^Future fut timeout-ms timeout-val]
   (try (.get fut timeout-ms TimeUnit/MILLISECONDS)
     (catch TimeoutException e
       timeout-val))))

(defn dummy-future-call
  "A dummy future-call that runs in serial and returns a future containing the
  result."
  [f]
  (let [result (f)]
    (reify
      clojure.lang.IDeref
      (deref [_] result)
      clojure.lang.IBlockingDeref
      (deref
        [_ timeout-ms timeout-val]
        result)
      clojure.lang.IPending
      (isRealized [_] true)
      Future
      (get [_] result)
      (get [_ timeout unit] result)
      (isCancelled [_] false)
      (isDone [_] true)
      (cancel [_ interrupt?] false))))

(defn validate-future-pool
  "Verify that a threadpool is a valid pool for a future."
  [pool]
  (when-not (or (= :serial pool)
                (= :builtin pool)
                (instance? ExecutorService pool))
    (throw (IllegalArgumentException.
             (format
               (str "Theadpool futures require a threadpool, :builtin, or "
                    ":serial, not %s.") pool)))))

(defonce ^{:doc "The previously-used threadpool ID."}
  threadpool-id
  ;; Start at -1 so we can just use the return value of (swap! inc).
  (atom -1))

(defn default-threadpool-name
  "The default name for threads in a threadpool. Gives each threadpool a
  unique ID via threadpool-id."
  []
  (format "claypoole-%d" (swap! threadpool-id inc)))

(defn apply-map
  "Apply a function that takes keyword arguments to a map of arguments."
  [f & args]
  (let [args (drop-last args)
        arg-map (last args)]
    (apply f (concat args (mapcat identity arg-map)))))

(defn thread-factory
  "Create a ThreadFactory with options including thread daemon status, the
  thread name format (a string for format with one integer), and a thread
  priority."
  ^ThreadFactory [& {:keys [daemon thread-priority] pool-name :name}]
  (let [daemon* (boolean daemon)
        pool-name* (or pool-name (default-threadpool-name))
        thread-priority* (or thread-priority
                             (.getPriority (Thread/currentThread)))]
    (let [default-factory (Executors/defaultThreadFactory)
          ;; The previously-used thread ID. Start at -1 so we can just use the
          ;; return value of (swap! inc).
          thread-id (atom -1)]
      (reify ThreadFactory
        (^Thread newThread [_ ^Runnable r]
          (let [t (.newThread default-factory r)]
            (doto t
              (.setDaemon daemon*)
              (.setName (str pool-name* "-" (swap! thread-id inc)))
              (.setPriority thread-priority*))))))))

(defn unchunk
  "Takes a seqable and returns a lazy sequence that is maximally lazy.

  Based on http://stackoverflow.com/questions/3407876/how-do-i-avoid-clojures-chunking-behavior-for-lazy-seqs-that-i-want-to-short-ci"
  [s]
  (when-let [s (seq s)]
    (cons (first s)
          (lazy-seq (unchunk (rest s))))))

(defn threadpool
  "Make a threadpool. It should be shutdown when no longer needed.

  See docs in com.climate.claypoole/threadpool.
  "
  [n & args]
  (let [factory (apply thread-factory args)]
    (Executors/newFixedThreadPool n factory)))

(defn- prioritize
  "Apply a priority function to a task.

  Note that this re-throws all priority-fn exceptions as ExecutionExceptions.
  That shouldn't mess anything up because the caller re-throws it as an
  ExecutionException anyway."
  [task, ^IFn priority-fn]
  (let [priority (try
                   (long (priority-fn (-> task meta :args)))
                   (catch Exception e
                     (throw (ExecutionException.
                              "Priority function exception" e))))]
    (reify
      Callable
      (call [_] (.call ^Callable task))
      Runnable
      (run [_] (.run ^Runnable task))
      Prioritized
      (getPriority [_] priority))))

;; A Threadpool that has a priority function.
(deftype PriorityThreadpool [^PriorityThreadpoolImpl pool, ^IFn priority-fn]
  ExecutorService
  (^boolean awaitTermination [_, ^long timeout, ^TimeUnit unit]
    (.awaitTermination pool timeout unit))
  (^List invokeAll [_, ^Collection tasks]
    (.invokeAll pool tasks))
  (^List invokeAll [_, ^Collection tasks, ^long timeout, ^TimeUnit unit]
    (.invokeAll pool tasks timeout unit))
  (^Object invokeAny [_, ^Collection tasks]
    (.invokeAny pool tasks))
  (^Object invokeAny [_, ^Collection tasks, ^long timeout, ^TimeUnit unit]
    (.invokeAny pool tasks timeout unit))
  (^boolean isShutdown [_]
    (.isShutdown pool))
  (^boolean isTerminated [_]
    (.isTerminated pool))
  (shutdown [_]
    (.shutdown pool))
  (^List shutdownNow [_]
    (.shutdownNow pool))
  (^Future submit [_, ^Runnable task]
    (.submit pool ^Runnable (prioritize task priority-fn)))
  (^Future submit [_, ^Runnable task, ^Object result]
    (.submit pool ^Runnable (prioritize task priority-fn) result))
  (^Future submit [_ ^Callable task]
    (.submit pool ^Callable (prioritize task priority-fn))))

(defn ->threadpool
  "Convert the argument into a threadpool, leaving the special keyword :serial
  alone.

  Returns [created? threadpool], where created? indicates whether a new
  threadpool was instantiated."
  [arg]
  (cond
    (instance? ExecutorService arg) [false arg]
    (integer? arg) [true (threadpool arg)]
    (= :builtin arg) [false clojure.lang.Agent/soloExecutor]
    (= :serial arg) [false :serial]
    :else (throw (IllegalArgumentException.
                   (format
                     (str "Claypoole functions require a threadpool, a "
                          "number, :builtin, or :serial, not %s.") arg)))))
