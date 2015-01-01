# Claypoole: Threadpool tools for Clojure

The claypoole library provides threadpool-based parallel versions of Clojure
functions such as `pmap`, `future`, and `for`.

Claypoole is available in the Clojars repository. Just use this leiningen
dependency: `[com.climate/claypoole "0.3.3"]`.

## Why do you use claypoole?

Claypoole gives us tools to deal with common parallelism issues by letting us
use and manage our own threadpools (a.k.a. thread pools).

Clojure has some nice tools for simple parallelism, but they're not a complete
solution for doing complex things (such as controlling the level of
parallelism). Instead, people tend to fall back on Java. For instance, on
[http://clojure.org/concurrent_programming](http://clojure.org/concurrent_programming),
the recommendation is to create an `ExecutorService` and call its `invokeAll`
method.

On the other hand, we do not need the flexible building blocks that are
[`core.async`](https://github.com/clojure/core.async);
we just want to run some simple tasks in parallel.  Similarly,
[`reducers`](http://clojure.org/reducers) is elegant, but we can't control its
level of parallelism.

Essentially, we wanted a `pmap` function that improves on the original in
several ways:

* We should be able to set the size of the threadpool `pmap` uses, so it would
  be tunable for non-CPU-bound tasks like network requests.
* We should be able to share a threadpool between multiple `pmap`s to control
  the amount of simultaneous work we're doing.
* We would like it to be eagerly streaming rather than lazy, so we can start it
  going and expect it to work in the background without explicitly consuming
  the results. *As of Claypoole 0.4.0, there are lazy maps in
  `com.climate.claypoole.lazy`. See the section [Lazy](#lazy) below.*
* We would like to be able to do an unordered `pmap`, so that we can start
  handling the first response as fast as possible.

## How do I use claypoole?

To use claypoole, make a threadpool via `threadpool` and use it with
claypoole's version of one of these standard Clojure functions:

* `future`
* `pmap`
* `pcalls`
* `pvalues`
* `for`

Instead of lazy sequences, our functions will return eagerly streaming
sequences. Such a sequence basically looks like `(map deref futures)` with a
thread in the background running `doall` on it, so all the requested work is
being done behind the scenes. Reading the sequence will only block on
uncompleted work.

Note that these functions are eager! That's on purpose--we like to be able to
start work going and know it'll get done. But if you try to `pmap` over
`(range)`, you're going to have a bad time.

*As of Claypoole 0.4.0, there are lazy functions in
`com.climate.claypoole.lazy`. See the section [Lazy](#lazy) below.*

```clojure
(require '[com.climate.claypoole :as cp])
;; A threadpool with 2 threads.
(def pool (cp/threadpool 2))
;; Future
(def fut (cp/future pool (myfn myinput)))
;; Ordered pmap
(def intermediates (cp/pmap pool myfn1 myinput-a myinput-b))
;; We can feed the streaming sequence right into another parallel function.
(def output (cp/pmap pool myfn2 intermediates))
;; We can read the output from the stream as it is available.
(doseq [o output] (prn o))
;; NOTE: The JVM doesn't automatically clean up threads for us.
(cp/shutdown pool)
```

Claypoole also contains functions for unordered parallelism. We found that for
minimizing latency, we often wanted to deal with results as soon as they became
available. This works well with our eager streaming, so we can chain together
streams using the same or different threadpools to get work done as quickly as
possible.

```clojure
(require '[com.climate.claypoole :as cp])
;; We'll use the with-shutdown! form to guarantee that pools are cleaned up.
(cp/with-shutdown! [net-pool (cp/threadpool 100)
                    cpu-pool (cp/threadpool (cp/ncpus))]
  ;; Unordered pmap doesn't return output in the same order as the input(!),
  ;; but that means we can start using service2 as soon as possible.
  (def service1-resps (cp/upmap net-pool service1-request myinputs))
  (def service2-resps (cp/upmap net-pool service2-request service1-resps))
  (def results (cp/upmap cpu-pool handle-response service2-resps))
  ;; ...eventually...
  ;; Make sure sure the computation is complete before we shutdown the pools.
  (doall results))
```

Claypoole provides ordered and unordered parallel `for` macros. Note that only
the bodies of the for loops will be run in parallel; everything in the for
binding will be done in the calling thread.

```clojure
(def ordered (cp/pfor pool [x xs
                            y ys]
               (myfn x y)))
(def unordered (cp/upfor pool [x xs
                               ;; This let is done in the calling thread.
                               :let [ys (range x)]
                               y ys]
                  (myfn x y)))
```

Claypoole also lets you prioritize your backlog of tasks. Higher-priority tasks
will be assigned to threads first. Here's an example; there is a more detailed
description below.

```clojure
(def pool (cp/priority-threadpool 10))
(def task1 (cp/future (cp/with-priority pool 1000) (myfn 1)))
(def task2 (cp/future (cp/with-priority pool 0) (myfn 2)))
(def moretasks (cp/pmap (cp/with-priority pool 10) myfn (range 3 10)))
```

## Do I really need to manage all those threadpools?

You don't need to specifically declare a threadpool. Instead, you can just give
a parallel function a number. Then, it will create its own private threadpool
and shut it down safely when all its tasks are complete.

```clojure
;; Use a temporary threadpool with 4 threads.
(cp/pmap 4 myfunction myinput)
;; Use a temporary threadpool with ncpus + 2 threads.
(cp/pmap (+ 2 (cp/ncpus)) myfunction myinput)
```

You can also pass the keyword `:builtin` as a threadpool. In that case, the
function will be run using Clojure's built-in, dynamically-scaling threadpool.
This is equivalent to running each task in its own `clojure.core/future`.

```clojure
;; Use built-in parallelism.
(def f (cp/future :builtin (myfn myinput)))
(def results (cp/pfor :builtin [x xs] (myfn x)))
```

You can also pass the keyword `:serial` as a threadpool. In that case, the
function will be run eagerly in series in the calling thread, not in parallel,
so the parallel function will not return until all the work is done. We find
this is helpful for testing and benchmarking. (See also about `*parallel*`
below.)

```clojure
;; Use no parallelism at all; blocks until all work is complete.
(cp/pmap :serial myfunction myinput)
```

## How do I dispose of my threadpools?

*As of Claypoole version 0.3, by default all threadpools are daemon
threadpools, so they should shut down when your main thread exits. But it's
still good practice to clean up these resources. Neither the OS nor the JVM
will take care of them for you!*

The JVM does not automatically garbage collect threads for you. Instead, when
you're done with your threadpool, you should use `shutdown` to gently shut down
the pool, or `shutdown!` to kill the threads forcibly.

Of course, we have provided a convenience macro `with-shutdown!` that will
`let` a threadpool and clean it up automatically:

```clojure
(cp/with-shutdown! [pool (cp/threadpool 3)]
  ;; Use the pool, confident it will be shut down when you're done. But be
  ;; careful--if work is still going on in the pool when you reach the end of
  ;; the body of with-shutdown!, it will be forcibly killed!
  ...)
```

Alternately, daemon threads will be automatically killed when the JVM process
exits. *By default all threadpools are daemon threadpools and will exit when
the main thread exits!* You can create a non-daemon threadpool via:

```clojure
(def pool (cp/threadpool 10 :daemon false))
```

## How do I set threadpool options?

To construct a threadpool, use the `threadpool` function. It takes optional
keyword arguments allowing you to change the thread names, their daemon status,
and their priority. (NOTE: Thread priority is [a system-level property that
depends on the
OS](http://oreilly.com/catalog/expjava/excerpt/#EXJ-CH-6-SECT-4); it is not the
same as the task priority, described below.)

```clojure
(def pool (cp/threadpool (cp/ncpus)
                         :daemon false
                         :thread-priority 3
                         :name "my-pool"))
```

## How can I disable threading?

We have found that benchmarking and some other tests are most easily done in
series. You can do that in one of two ways.

First, you can just pass the keyword `:serial` to a parallel function.

```clojure
(def results (cp/pmap :serial myfn inputs))
```

Second, you can bind or `with-redefs` the variable `cp/*parallel*` to false,
like so:

```clojure
(binding [cp/*parallel* false]
  (with-shutdown! [pool (cp/threadpool 2)]
    ;; This is in series; we block until all work is complete!
    (cp/pmap pool myfn inputs)))
```

## <a name="lazy">Lazy</a>

As of Claypoole 0.4.0, there is a namespace `com.climate.claypoole.lazy` that
contains lazy versions of all the parallel functions. These lazy versions do
not compute work until forced by something like `(doall)`, just like core `map`
and `pmap`.

Like core `pmap`, they will only run the outputs you realize, plus a few more
(a buffer). The buffer is used to help keep the threadpool busy. Each parallel
function also comes with a -buffer variant that allows you to specify the
buffer size. The non -buffer forms use the threadpool size as their buffer
size.

For instance, these will both cause 10 items to be realized:

```clojure
(cp/with-shutdown! [pool 2]
  (doall (take 8 (lazy/pmap pool inc (range))))
  (doall (take 4 (lazy/pmap-buffer pool 6 inc (range)))))
```

The disadvantage of the lazy functions is that they may not keep the threadpool
as busy. For instance, this pmap will take 6 milliseconds to run:

```clojure
(lazy/pmap 2 #(Thread/sleep %) [4 3 2 1])
```

That's because it will not realize the 2 task until the 4 task is complete, so
one thread in the pool will sit idle for 1 millisecond.

To use the threadpool most efficiently with these lazy functions, prefer the
unordered versions (e.g. `upmap`), since the ordered ones may starve the
threadpool of work.

## How can I prioritize my tasks?

You can create a threadpool that respects task priorities by creating a
`priority-threadpool`:

```clojure
(def p1 (cp/priority-threadpool 5))
(def p2 (cp/priority-threadpool 5 :default-priority -10))
```

Then, use functions `with-priority` and `with-priority-fn` to set the priority
of your tasks, or just set the `:priority` in your `for` loop:

```clojure
(cp/future (cp/with-priority p1 100) (myfn))
;; Nothing bad happens if you nest with-priority. The outermost one "wins";
;; this task runs at priority 2.
(cp/future (cp/with-priority (cp-with-priority p1 1) 2) (myfn))
;; For pmaps, you can use a priority function, which is called with your
;; arguments. This will run 3 tasks at priorities 6, 5, and 4, respectively.
(cp/upmap (cp/with-priority-fn p1 (fn [x _] x)) + [6 5 4] [1 2 3])
;; For for loops, you can use the special :priority binding, which must be the
;; last for binding.
(cp/upfor p1 [i (range 10)
              :priority (- i)]
  (myfn i))
```

## What about Java interoperability?

Under the hood, threadpools are just instances of
`java.util.concurrent.ExecutorService`. You can use any `ExecutorService` in
place of a threadpool, and you can use a threadpool just as you would an
`ExecutorService`. This means you can create custom threadpools and use them
easily.

You might also like using [digiriste](https://github.com/ztellman/dirigiste)
threadpools with Claypoole. Digiriste provides a fast, instrumented
ExecutorService that scales in a controllable way.

## OMG My program isn't exiting!

There are a few cases where threadpools will stop your program from exiting
that can surprise users. We have endeavored to minimize them, but they can
still be problems.

### My program doesn't exit until 60 seconds after main exits.

Claypoole actually uses some `clojure.core/future`s.  Unfortunately, those
threads are from the agent threadpool, and they are not daemon threads. Those
threads will automatically die 60 seconds after they're used. So if your main
thread doesn't call either `shutdown-agents` or `System/exit`, those threads
will keep going for a while!

You'll [experience the same
thing](http://tech.puredanger.com/2010/06/08/clojure-agent-thread-pools/) if
you start a `future` or `pmap`--those extra threads have to be explicitly shut
down, or they'll naturally die after 60 seconds.

The answer is basically to call `(shutdown-agents)` before your main thread
exits.

_In a future version of Claypoole, we may switch to using a separate daemon
threadpool for those futures. However, that could have other complications, so
we are deferring that decision._

### My program doesn't exit ever!

Claypoole now (as of version 0.3) defaults to daemon threadpools, but before
that threadpools were not daemons. If you have any non-daemon threadpools
running and your main process exits, those threadpools will never die, and your
program will hang indefinitely!

The answer is: either use daemon threadpools (which is now the default) or be
very careful to shut down your threadpools when you're done with them. See the
above section on disposing of threadpools.

## Why the name "Claypoole"?

The claypoole library is named after [John Claypoole (Betsy Ross's third
husband)](http://en.wikipedia.org/wiki/Betsy_Ross) for reasons that are at
best obscure.

## License

Copyright (C) 2014 The Climate Corporation. Distributed under the Apache
License, Version 2.0.  You may not use this library except in compliance with
the License. You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

See the NOTICE file distributed with this work for additional information
regarding copyright ownership.  Unless required by applicable law or agreed
to in writing, software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
or implied.  See the License for the specific language governing permissions
and limitations under the License.
