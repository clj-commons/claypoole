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

(ns com.climate.claypoole-test
  (:require
    [clojure.test :refer :all]
    [com.climate.claypoole :as cp]
    [com.climate.claypoole.impl :as impl])
  (:import
    [com.climate.claypoole.impl
     ;; Import to make Eastwood happy.
     PriorityThreadpool]
    [java.util.concurrent
     ExecutionException
     ExecutorService]))


(defn callable
  "Just a cast."
  ^Callable [^clojure.lang.IFn f]
  f)

(defn check-threadpool-options
  [pool-constructor]
  (cp/with-shutdown! [pool (pool-constructor 4)]
    (dotimes [_ 8] (.submit pool #(inc 1)))
    (let [factory (.getThreadFactory pool)
          start (promise)
          thread (.newThread factory #(deref start))]
      (is (true? (.isDaemon thread)))
      (is (not (empty? (re-find #"claypoole-[0-9]*-4" (.getName thread)))))
      (is (= (.getPriority (Thread/currentThread)) (.getPriority thread)))))
  (cp/with-shutdown! [pool (pool-constructor 4
                                             :daemon false
                                             :name "fiberpond"
                                             :thread-priority 4)]
    (dotimes [_ 8] (.submit pool #(inc 1)))
    (let [factory (.getThreadFactory pool)
          start (promise)
          thread (.newThread factory #(deref start))]
      (is (false? (.isDaemon thread)))
      (is (= "fiberpond-4" (.getName thread)))
      (is (= 4 (.getPriority thread))))))

(deftest test-threadpool
  (testing "Basic threadpool creation"
    (cp/with-shutdown! [pool (cp/threadpool 4)]
      (is (instance? ExecutorService pool))
      (dotimes [_ 8] (.submit pool #(inc 1)))
      (is (= 4 (.getPoolSize pool)))))
  (testing "Threadpool options"
    (check-threadpool-options cp/threadpool)))

(defn- sorted*?
  "Is a sequence sorted?"
  [x]
  (= x (sort x)))

(deftest test-priority-threadpool
  (testing "Priority threadpool ordering is mostly in order"
    (cp/with-shutdown! [pool (cp/priority-threadpool 1)]
      (let [start (promise)
            completed (atom [])
            tasks (doall
                    (for [i (range 10)]
                      (cp/future (cp/with-priority pool i)
                                 (deref start)
                                 (swap! completed conj i))))]
        ;; start tasks
        (Thread/sleep 100)
        (deliver start true)
        ;; Wait for tasks to complete
        (doseq [f tasks] (deref f))
        (is (= [0 9 8 7 6 5 4 3 2 1]
               @completed)))))
  (testing "Priority threadpool ordering is ordered with unordered inputs."
    (cp/with-shutdown! [pool (cp/priority-threadpool 1)]
      (let [start (promise)
            completed (atom [])
            tasks (doall
                    (for [i (shuffle (range 100))]
                      (cp/future (cp/with-priority pool i)
                                 (deref start)
                                 (swap! completed conj i))))]
        ;; start tasks
        (deliver start true)
        ;; Wait for tasks to complete
        (doseq [f tasks] (deref f))
        (is (sorted*?
              (-> completed
                  deref
                  ;; The first task will be one at random, so drop it
                  rest
                  reverse))))))
  (testing "Priority threadpool default priority."
    (cp/with-shutdown! [pool (cp/priority-threadpool 1 :default-priority 50)]
      (let [start (promise)
            completed (atom [])
            run (fn [result] (deref start) (swap! completed conj result))
            first-task (cp/future pool (run :first))
            tasks (doall
                    (for [i [1 100]]
                      (cp/future (cp/with-priority pool i) (run i))))
            default-task (cp/future pool (run :default))]
        ;; start tasks
        (deliver start true)
        ;; Wait for tasks to complete
        (doseq [f tasks] (deref f))
        (deref default-task)
        (is (= [:first 100 :default 1] @completed)))))
  (testing "Priority threadpool options"
    (check-threadpool-options cp/threadpool)))

(deftest test-with-priority-fn
  (testing "with-priority-fn works for simple upmap"
    (cp/with-shutdown! [pool (cp/priority-threadpool 1)]
      (let [start (promise)
            results (cp/upmap (cp/with-priority-fn
                                pool (fn [& args] (first args)))
                              (fn [i]
                                (deref start)
                                i)
                              (range 10))]
        ;; start tasks
        (Thread/sleep 100)
        (deliver start true)
        (is (= [0 9 8 7 6 5 4 3 2 1]
               results)))))
  (testing "with-priority-fn throws sensible exceptions"
    (cp/with-shutdown! [pool (cp/priority-threadpool 2)]
      (is (thrown-with-msg?
            Exception #"Priority function exception"
            ;; Arity exception.
            (dorun
              (cp/pmap (cp/with-priority-fn pool (fn [] 0))
                       (fn [x y] (+ x y))
                       (range 10) (range 10)))))
      (is (thrown-with-msg?
            ;; No arguments passed to priority function.
            Exception #"Priority function exception"
            (deref (cp/future (cp/with-priority-fn
                                pool (fn [& args] (first args)))
                              1)))))))

(deftest test-for-priority
  (testing "pfor uses priority"
    (cp/with-shutdown! [pool (cp/priority-threadpool 1)]
      (let [start (promise)
            completed (atom [])
            tasks (cp/pfor pool
                    [i (range 100)
                     :priority (inc i)]
                    (deref start)
                    (swap! completed conj i)
                    i)]
        (Thread/sleep 100)
        (deliver start true)
        (dorun tasks)
        ;; Just worry about the rest of the tasks; the first one may be out of
        ;; order.
        (is (sorted*? (reverse (rest @completed))))
        (is (= (range 100) tasks)))))
  (testing "upfor uses priority"
    (cp/with-shutdown! [pool (cp/priority-threadpool 1)]
      (let [start (promise)
            completed (atom [])
            tasks (cp/upfor pool
                    [i (range 100)
                     :priority (inc i)]
                    (deref start)
                    (swap! completed conj i)
                    i)]
        (Thread/sleep 100)
        (deliver start true)
        (dorun tasks)
        ;; Just worry about the rest of the tasks; the first one may be out of
        ;; order.
        (is (sorted*? (reverse (rest @completed))))
        (is (= @completed tasks))))))

(deftest test-priority-nonIObj
  (testing "A priority pool should work on any sort of Callable."
    (cp/with-shutdown! [pool (cp/priority-threadpool 1)]
      (let [start (promise)
            results (atom [])
            run (fn [x] (deref start) (swap! results conj x))]
        ;; Dummy task, always runs first.
        (cp/future (cp/with-priority pool 100)
                   (run 100))
        ;; Runnables.
        (.submit (cp/with-priority pool 1)
                 (reify Runnable (run [_] (run 1))))
        (.submit (cp/with-priority pool 10)
                 (reify Runnable (run [_] (run 10))))
        ;; Runnables with return value.
        (.submit (cp/with-priority pool 2)
                 (reify Runnable (run [_] (run 2)))
                 :return-value)
        (.submit (cp/with-priority pool 9)
                 (reify Runnable (run [_] (run 9)))
                 :return-value)
        (cp/future (cp/with-priority pool 6)
                   (run 6))
        (cp/future (cp/with-priority pool 11)
                   (run 11))
        ;; Callables
        (.submit (cp/with-priority pool 3)
                 (reify Callable (call [_] (run 3))))
        (.submit (cp/with-priority pool 8)
                 (reify Callable (call [_] (run 8))))
        ;; And another couple IFns for good measure
        (cp/future (cp/with-priority pool 5)
                   (run 5))
        (cp/future (cp/with-priority pool 7)
                   (run 7))
        ;; Make them go.
        (Thread/sleep 100)
        (deliver start true)
        ;; Check the results
        (Thread/sleep 100)
        (is (sorted*? (reverse @results)))))))

(deftest test-threadpool?
  (testing "Basic threadpool?"
    (cp/with-shutdown! [pool 4
                        priority-pool (cp/priority-threadpool 4)]
      (is (true? (cp/threadpool? pool)))
      (is (true? (cp/threadpool? priority-pool)))
      (is (false? (cp/threadpool? :serial)))
      (is (false? (cp/threadpool? nil)))
      (is (false? (cp/threadpool? 1))))))

(deftest test-shutdown
  (testing "Basic shutdown"
    (let [pool (cp/threadpool 4)
          start (promise)
          result (promise)
          f (.submit pool (callable #(deliver result (deref start))))]
      (is (false? (cp/shutdown? pool)))
      (Thread/sleep 100)
      ;; Make sure the threadpool starts shutting down but doesn't complete
      ;; until the threads finish.
      (cp/shutdown pool)
      (is (true? (cp/shutdown? pool)))
      (is (false? (.isTerminated pool)))
      (Thread/sleep 100)
      (deliver start true)
      (Thread/sleep 100)
      (is (true? (.isTerminated pool)))
      (is (true? @result))))
  (testing "Shutdown does not affect builtin threadpool"
    (cp/shutdown clojure.lang.Agent/soloExecutor)
    (is (not (cp/shutdown? clojure.lang.Agent/soloExecutor)))))

(deftest test-shutdown!
  (testing "Basic shutdown!"
    (let [pool (cp/threadpool 4)
          start (promise)
          f (.submit pool (callable #(deref start)))]
      (is (false? (cp/shutdown? pool)))
      (Thread/sleep 100)
      ;; Make sure the threadpool completes shutting down immediately.
      (cp/shutdown! pool)
      (is (true? (cp/shutdown? pool)))
      ;; It can take some time for the threadpool to kill the threads.
      (Thread/sleep 100)
      (is (true? (.isTerminated pool)))
      (is (.isDone f))
      (is (thrown? ExecutionException (deref f)))))
  (testing "Shutdown! does not affect builtin threadpool"
    (cp/shutdown! clojure.lang.Agent/soloExecutor)
    (is (not (cp/shutdown? clojure.lang.Agent/soloExecutor)))))

(deftest test-with-shutdown!
  (testing "With-shutdown! arguments"
    (doseq [arg [4 (cp/threadpool 4) :builtin :serial]]
      (let [outside-pool (promise)
            start (promise)
            fp (promise)]
        (cp/with-shutdown! [pool arg]
          (deliver outside-pool pool)
          ;; Use a future to avoid blocking on the :serial case.
          (deliver fp (future (.submit pool #(deref start))))
          (Thread/sleep 100))
        ;; Make sure outside of the with-shutdown block the pool is properly
        ;; killed.
        (when-not (keyword? arg) (is (true? (cp/shutdown? @outside-pool)))
          (Thread/sleep 100)
          (is (true? (.isTerminated @outside-pool)))
          (deliver start true)
          (Thread/sleep 100)
          (is (.isDone @@fp))
          (is (thrown? ExecutionException (deref @@fp)))))))
  (testing "With-shutdown! works with any number of threadpools"
    (let [input (range 100)]
      (is (= input
             (cp/with-shutdown! []
               (map identity input))))
      (is (= input
             (cp/with-shutdown! [p1 4]
               (->> input
                    (cp/pmap p1 identity)
                    doall))))
      (is (= input
             (cp/with-shutdown! [p1 4
                                 p2 3]
               (->> input
                    (cp/pmap p1 identity)
                    (cp/pmap p2 identity)
                    doall))))
      (is (= input
             (cp/with-shutdown! [p1 4
                                 p2 3
                                 p3 5]
               (->> input
                    (cp/pmap p1 identity)
                    (cp/pmap p2 identity)
                    (cp/pmap p3 identity)
                    doall))))))
  (testing "Invalid with-shutdown! arguments"
    (is (thrown? IllegalArgumentException
                 (cp/with-shutdown! [pool 1.5] nil)))
    (is (thrown? IllegalArgumentException
                 (cp/with-shutdown! [pool :parallel] nil)))))

(defn check-parallel
  "Check that a pmap function actually runs in parallel."
  [pmap-like ordered? & [lazy?]]
  (let [n 10]
    (cp/with-shutdown! [pool n]
      (let [pool (cp/threadpool n)
            ;; Input is just a sequence of numbers. It's not in order so we can
            ;; check the streaming properties of the parallel function.
            input (vec (reverse (range n)))
            ;; We'll record what tasks have been started so we can make sure
            ;; all of them are started.
            started (atom [])
            ;; We'll check that our responses are streamed as available. We'll
            ;; control the order they're available with a sequence of promises.
            promise-chain (vec (repeatedly n promise))
            results (pmap-like pool
                               ;; Use a fancy identity function that waits on
                               ;; the ith element in the chain of promises to
                               ;; start.
                               (fn [i]
                                 ;; Log that this task has been started.
                                 (swap! started conj i)
                                 ;; Wait for our promise to be ready.
                                 (deref (promise-chain i))
                                 ;; Sleep a little to make sure that this task
                                 ;; returns noticeably later than the previous
                                 ;; one.
                                 (Thread/sleep 1)
                                 ;; Tell the next task it can run.
                                 (when (< (inc i) n)
                                   (deliver (promise-chain (inc i)) i))
                                 i)
                               input)]
        ;; If it's a truly lazy function, we have to force the sequence to make
        ;; the futures start.
        (when lazy? (future (doall results)))
        ;; All tasks should have started after 100ms.
        (Thread/sleep 100)
        (is (= (set @started) (set input)))
        ;; Each task should be started only once.
        (is (= @started (distinct @started)))
        ;; Start the first task.
        (deliver (first promise-chain) nil)
        (is (= results (if ordered?
                         ;; If we're doing an ordered operation, we expect to
                         ;; see them in the order we submitted them.
                         input
                         ;; If we're doing an -unordered operation, we expect
                         ;; to see them return as available, so in sorted
                         ;; order.
                         (sort input))))))))

(defn check-lazy-read
  "Check that a pmap function reads lazily"
  [pmap-like & [lazy?]]
  (let [n 10]
    (cp/with-shutdown! [pool n]
      (let [pool (cp/threadpool n)
            first-inputs (range n)
            second-inputs (range n (* n 2))
            ;; The input will have a pause after n items.
            pause (promise)
            input (concat first-inputs (lazy-seq (list @pause)) second-inputs)
            ;; We'll record what tasks have been started so we can make sure
            ;; all of them are started.
            started (atom #{})
            results (pmap-like pool
                               (fn [i] (swap! started conj i) i)
                               input)]
        ;; When genuinely lazy, we must force the sequence to start tasks.
        (when lazy? (future (doall results)))
        ;; All of the first set of tasks should have started after 100ms.
        (Thread/sleep 100)
        (is (= @started (set first-inputs)))
        (deliver pause :pause)
        (Thread/sleep 100)
        (is (= @started (set results) (set input)))))))

(defn check-chaining
  "Check that we can chain pmaps."
  [pmap-like]
  (cp/with-shutdown! [p1 (cp/threadpool 2)
                      p2 (cp/threadpool 4)]
    (is (= (range 1 11)
           (->> (range 10)
                (pmap-like p1 inc)
                sort)))
    (is (= (range 2 12)
           (->> (range 10)
                (pmap-like p1 inc)
                (pmap-like p1 inc)
                sort)))
    (is (= (range 3 13)
           (->> (range 10)
                (pmap-like p1 inc)
                (pmap-like p1 inc)
                (pmap-like p2 inc)
                sort)))))

(defn check-shutdown-exceptions
  "Check that exceptions are thrown when tasks go to a shutdown pool."
  [pmap-like]
  (cp/with-shutdown! [pool 2]
    (let [input (range 4)
          start (promise)
          delayed-input (map (fn [i] (deref start) i) input)
          results (future (pmap-like pool identity
                                     (concat input delayed-input)))]
      (Thread/sleep 100)
      (cp/shutdown pool)
      (deliver start true)
      (is (thrown? Exception (dorun @results))))))

(defn check-fn-exception
  "Check that a pmap function correctly passes exceptions caused by the
  function."
  [pmap-like]
  (let [n 10
        pool (cp/threadpool n)
        inputs [0 1 2 3 :4 5 6 7 8 9]]
    (is (thrown-with-msg?
          NullPointerException #"keyword found"
          (dorun (pmap-like pool
                            (fn [i]
                              (if (keyword? i)
                                (throw (NullPointerException. "keyword found"))
                                i))
                            inputs))))
    (.shutdown pool)))

(defn check-fn-throwable
  "Check that a pmap function correctly passes non-Exception throwables caused
  by the function."
  [pmap-like]
  (let [n 10
        pool (cp/threadpool n)
        inputs [0 1 2 3 :4 5 6 7 8 9]]
    (is (thrown-with-msg?
          AssertionError #"keyword found"
          (dorun (pmap-like pool
                            (fn [i]
                              (if (keyword? i)
                                (throw (AssertionError. "keyword found"))
                                i))
                            inputs))))
    (.shutdown pool)))

(defn check-input-exception
  "Check that a pmap function correctly passes exceptions caused by lazy
  inputs."
  [pmap-like]
  (let [n 10
        pool (cp/threadpool n)
        inputs (map #(if (< % 100)
                       %
                       (throw (Exception.
                                "deliberate exception")))
                    (range 200))]
    (is (thrown-with-msg?
          Exception #"deliberate"
          (dorun (pmap-like pool inc inputs))))
    (.shutdown pool)))

(defn check-maximum-parallelism-one-case
  "Check that a pmap function doesn't exhibit excessive parallelism."
  [pmap-like n pool]
  (let [ni (min 100 (* n 10))  ;; Don't test too many cases.
        inputs (range ni)
        ;; Keep track of what threads are active.
        n-active (atom 0)
        n-seen-active (atom [])
        results (pmap-like pool
                           (fn [i]
                             (swap! n-active inc)
                             (Thread/sleep 1)
                             ;; Make sure not too many threads are going.
                             (swap! n-seen-active conj @n-active)
                             (swap! n-active dec)
                             i)
                           inputs)]
    (is (= (sort results) inputs))
    (is (every? #(<= % n) @n-seen-active))))

(defn check-maximum-parallelism
  "Check that a pmap function doesn't exhibit excessive parallelism."
  [pmap-like]
  (doseq [[pool n shutdown?]
          [[(cp/threadpool 10) 10 true]
           [10 10 false]
           [:builtin Integer/MAX_VALUE false]
           [:serial 1 false]]]
    (try (check-maximum-parallelism-one-case
           pmap-like n pool)
      (finally (when shutdown? (.shutdown pool))))))

(defn check-*parallel*-disables
  "Check that binding cp/*parallel* can disable parallelism."
  [pmap-like]
  (binding [cp/*parallel* false]
    (cp/with-shutdown! [pool 10]
      (check-maximum-parallelism-one-case pmap-like 1 pool))))

(defn check-->threadpool
  "Check that a pmap function uses ->threadpool correctly, shutting down the
  threadpool and everything."
  [pmap-like]
  (is (thrown? IllegalArgumentException (pmap-like 1.5 identity [1])))
  (is (thrown? IllegalArgumentException (pmap-like :parallel identity [1])))
  (let [real->threadpool impl/->threadpool
        apool (atom nil)
        n 4]
    (with-redefs [impl/->threadpool (fn [arg]
                                      (let [[s? p] (real->threadpool arg)]
                                        (reset! apool p)
                                        [s? p]))]
      (doseq [[is-pool? should-be-shutdown? arg inputs should-we-shutdown?]
              [[true false (cp/threadpool n) (range (* 2 n)) true]
               [true true n (range (* 2 n)) false]
               [true false :builtin (range (* 2 n)) false]
               [false false :serial (range (* 2 n)) false]]]
        (let [inputs (range (* n 2))
              ;; Use a real future to avoid blocking on :serial.
              results (future (pmap-like arg inc inputs))]
          ;; Check the results
          (is (= (map inc inputs) (sort @results)))
          (when should-be-shutdown?
            (is (true? (cp/shutdown? @apool))))
          (when should-we-shutdown?
            (cp/shutdown! @apool))))
      ;; Shut down even if an exception is thrown
      (is (thrown? Exception
                   (dorun (pmap-like 4 inc [1 2 nil]))))
      (is (true? (cp/shutdown? @apool))))))

;; A simple object to call a function at finalize.
(deftype Finalizer [f]
  Object
  (finalize [_] (f)))

(defn check-holding-thread
  "Verify that this pmap function does not hold onto the head of the sequence,
  so if no one else uses the results, they're garbage collected.

  Note: This test assumes that calling (System/gc) will cause un-referenced
  objects to be finalized. So far, that seems to be true, though the JVM does
  not guarantee that as a contract."
  [pmap-fn]
  (let [a (atom nil)
        started (promise)
        finish (promise)
        task-runner (future
                      (dorun
                        (pmap-fn 1 deref
                                 (list
                                   ;; Have one task make a note when GC'd
                                   (delay
                                     (Finalizer.
                                       #(reset! a :finalized)))
                                   (delay 1)
                                   (delay 2)
                                   (delay (deliver started :started))
                                   finish))))]
    ;; Let the tasks run
    @started
    ;; Trigger GC
    (System/gc)
    ;; Wait for GC to run
    (doseq [i (range 100)
            :while (not @a)]
      (Thread/sleep 1))
    ;; Verify that the task was GC'd
    (is (= @a :finalized))
    ;; Complete the map
    (deliver finish :done)
    @task-runner))

(defn check-read-ahead
  "Verify that this pmap function does not read too far ahead in the input
  sequence, as that can cause unnecessary use of RAM."
  [pmap-fn & [lazy?]]
  (let [a (atom nil)
        indicator #(do (reset! a %) a)
        finish (promise)
        started (promise)
        results (pmap-fn 4 deref
                         (concat ;; indicate we've started
                                 (repeatedly 1 #(do (deliver started true)
                                                  started))
                                 ;; block the map
                                 (repeat 10 finish)
                                 ;; a long runway
                                 (map atom (range 100))
                                 ;; an indicator for whether we've realized
                                 ;; past the runway
                                 (map indicator [:started])))]
    ;; When genuinely lazy, we must force the sequence to start tasks.
    (when lazy? (future (doall results)))
    ;; Let the tasks run
    @started
    ;; Let the threadpool run unchecked for a minute
    (Thread/sleep 100)
    ;; Verify that the indicator wasn't triggered
    (is (= nil @a))
    ;; Complete the map
    (deliver finish :done)
    (dorun results)))

(defn check-shuts-off
  [pmap-like]
  (cp/with-shutdown! [pool 2]
    (let [;; A lazy blocking trap
          blocker (map deref [(promise)])
          started (atom [])
          f (fn [x]
              (Thread/sleep 1)
              (swap! started conj x)
              (when (= x 10)
                (throw (Exception. (str x))))
              x)
          results (pmap-like pool f (concat (impl/unchunk (range 200)) blocker))]
      (Thread/sleep 300)
      ;; Surely we hit the exception.
      (is (thrown? Exception (dorun results)))
      ;; Surely we didn't start every task.
      (is (< 1 (count @started) 100))
      ;; Surely something, but not everything, is returned, and we do not
      ;; block.
      (is (< 1 (count results) 100)))))

(defn check-all
  "Run all checks on a pmap function.

  Arguments:
    fn-name     - the function's name for better logging/output
    pmap-like   - a function that works like pmap
    ordered?    - true iff the pmap function returns results in the same order
    streaming?  - true iff the pmap function works on streaming sequences
    lazy?       - true iff the pmap function needs to be \"pumped\" by doall"
  [fn-name pmap-like ordered? streaming? lazy?]
  (testing (format "%s maps" fn-name)
    (is (= (range 1 11) ((if ordered? identity sort)
                         (pmap-like 3 inc (range 10))))))
  (testing (format "%s runs n things at once" fn-name)
    (check-parallel pmap-like ordered? lazy?))
  (testing (format "%s emits exceptions correctly" fn-name)
    (check-fn-exception pmap-like))
  (testing (format "%s emits non-Exception Throwables correctly" fn-name)
    (check-fn-throwable pmap-like))
  (testing (format "%s handles input exceptions correctly" fn-name)
    (check-input-exception pmap-like))
  (testing (format "%s runs n things at once" fn-name)
    (check-maximum-parallelism pmap-like))
  (testing (format "%s uses ->threadpool correctly" fn-name)
    (check-->threadpool pmap-like))
  (testing (format "%s is made serial by binding cp/*parallel* to false"
                   fn-name)
    (check-*parallel*-disables pmap-like))
  (testing (format "%s throws exceptions when tasks are sent to a shutdown pool"
                   fn-name)
    (check-shutdown-exceptions pmap-like))
  (when streaming?
    (testing (format "%s doesn't hold the head of streaming sequences" fn-name)
      (check-holding-thread pmap-like))
    (testing (format "%s doesn't read ahead in the input sequence" fn-name)
      (check-read-ahead pmap-like lazy?))
    (testing (format "%s can be chained in various threadpools" fn-name)
      (check-chaining pmap-like))
    (testing (format "%s stops processing when an exception occurs" fn-name)
      (check-shuts-off pmap-like))
    (testing (format "%s reads lazily" fn-name)
      (check-lazy-read pmap-like lazy?))))


(deftest test-daemon
  (let [daemon? (fn [& args] (.isDaemon (Thread/currentThread)))]
    (testing "threadpools are daemon by default"
      (cp/with-shutdown! [pool 3]
        (is (every? boolean
                    (cp/pmap pool daemon? (range 100))))))
    (testing "threadpools are daemon by default"
      (cp/with-shutdown! [pool (cp/threadpool 3)]
        (is (every? boolean
                    (cp/pmap pool daemon? (range 100))))))
    (testing "we can make non-daemon threadpools"
      (cp/with-shutdown! [pool (cp/threadpool 3 :daemon false)]
        (is (not-any? boolean
                      (cp/pmap pool daemon?  (range 100))))))))

(deftest test-future
  (testing "basic future test"
    (cp/with-shutdown! [pool 3]
      (let [a (atom false)
            f (cp/future
                pool
                ;; Body can contain multiple elements.
                (reset! a true)
                (range 10))]
        (is (= @f (range 10)))
        (is (true? @a)))))
  (testing "future threadpool args"
    (is (thrown? IllegalArgumentException (cp/future 3 (inc 1))))
    (is (thrown? IllegalArgumentException (cp/future nil (inc 1))))
    (is (= 2 @(cp/future :builtin (inc 1))))
    (is (= 2 @(cp/future :serial (inc 1)))))
  (letfn [(pmap-like [pool work input]
            (map impl/deref-fixing-exceptions
                 (doall
                   (for [i input]
                     (cp/future pool (work i))))))]
    (testing "future runs simultaneously"
      (check-parallel pmap-like true))
    (testing "future throws exceptions okay"
      (check-fn-exception pmap-like))
    (testing "future throws exceptions okay"
      (check-fn-throwable pmap-like))
    (testing "future doesn't do too much parallelism"
      ;; We don't check the number or nil cases because future doesn't accept
      ;; those.
      (doseq [[pool n shutdown?]
              [[(cp/threadpool 10) 10 true]
               [:serial 1 false]]]
        (check-maximum-parallelism-one-case
          pmap-like n pool)
        (when shutdown? (.shutdown pool))))
    (testing "Binding cp/*parallel* can disable parallelism in future"
      (check-*parallel*-disables pmap-like))
    (testing "Future throws exceptions when tasks are sent to a shutdown pool"
      (check-shutdown-exceptions pmap-like))
    (testing "Futures can be chained in various threadpools."
      (check-chaining pmap-like))))

(deftest test-pmap
  (check-all "pmap" cp/pmap true true false))

(deftest test-upmap
  (check-all "upmap" cp/upmap false true false))

(deftest test-pcalls
  (testing "basic pcalls test"
    (cp/with-shutdown! [pool 3]
      (is (= [1 2 3 4]
             (cp/pcalls pool #(inc 0) #(inc 1) #(inc 2) #(inc 3))))))
  (letfn [(pmap-like [pool work input]
            (apply
              cp/pcalls
              pool
              (for [i input]
                #(work i))))]
    (check-all "pcalls" pmap-like true true false)))

(deftest test-upcalls
  (testing "basic pcalls test"
    (cp/with-shutdown! [pool 3]
      (is (= [1 2 3 4]
             (sort (cp/upcalls pool #(inc 0) #(inc 1) #(inc 2) #(inc 3)))))))
  (letfn [(pmap-like [pool work input]
            (apply
              cp/upcalls
              pool
              (for [i input]
                #(work i))))]
    (check-all "upcalls" pmap-like false true false)))

(deftest test-pvalues
  (testing "basic pvalues test"
    (cp/with-shutdown! [pool 3]
      (is (= [1 2 3 4]
             (cp/pvalues pool (inc 0) (inc 1) (inc 2) (inc 3))))))
  (letfn [(pmap-like [pool work input]
            (let [worksym (gensym "work")]
              ((eval
                 `(fn [pool# ~worksym]
                    (cp/pvalues
                      pool#
                      ~@(for [i input]
                          (list worksym i)))))
                 pool work)))]
    (check-all "pvalues" pmap-like true false false)))

(deftest test-upvalues
  (testing "basic upvalues test"
    (cp/with-shutdown! [pool 3]
      (is (= [1 2 3 4]
             (sort (cp/upvalues pool (inc 0) (inc 1) (inc 2) (inc 3)))))))
  (letfn [(pmap-like [pool work input]
            (let [worksym (gensym "work")]
              ((eval
                 `(fn [pool# ~worksym]
                    (cp/upvalues
                      pool#
                      ~@(for [i input]
                          (list worksym i)))))
                 pool work)))]
    (check-all "upvalues" pmap-like false false false)))

(deftest test-pfor
  (testing "basic pfor test"
    (cp/with-shutdown! [pool 3]
      (is (= (range 1 11)
             (cp/pfor pool [i (range 10)] (inc i))))))
  (letfn [(pmap-like [pool work input]
            (cp/pfor
              pool
              [i input]
              (work i)))]
    (check-all "pfor" pmap-like true true false)))

(deftest test-upfor
  (testing "basic upfor test"
    (cp/with-shutdown! [pool 3]
      (is (= (range 1 11)
             (sort (cp/pfor pool [i (range 10)] (inc i)))))))
  (letfn [(pmap-like [pool work input]
            (cp/upfor
              pool
              [i input]
              (work i)))]
    (check-all "upfor" pmap-like false true false)))
