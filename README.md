# Claypoole: Threadpool tools for Clojure

The claypoole library provides threadpool-based parallel versions of Clojure
functions such as `pmap`, `future`, and `for`.

Claypoole is available in the Clojars repository. Just use this leiningen
dependency:
[![Clojars Project](http://clojars.org/com.climate/claypoole/latest-version.svg)](http://clojars.org/com.climate/claypoole)
[![Dependencies Status](http://jarkeeper.com/TheClimateCorporation/claypoole/status.svg)](http://jarkeeper.com/TheClimateCorporation/claypoole)

Docs at
[![cljdoc badge](https://cljdoc.org/badge/com.climate/claypoole)](https://cljdoc.org/d/com.climate/claypoole/CURRENT)

## Why do you use claypoole?

Claypoole gives us tools to deal with common parallelism issues by letting us
use and manage our own threadpools (a.k.a. thread pools). Our [blog
post](doc/BLOG.md)
gives a nice overview of the project and its motivations.

Also, if you like learning via video, Leon gave [a talk at Clojure West
2015](https://www.youtube.com/watch?v=BzKjIk0vgzE). It gives an intro to how
Clojure parallelism works, including describing some of the motivations for
Claypoole. It describes what advantages Claypoole gives and also talks about
some of the other useful tools for Clojure parallelism (e.g.
[core.async](https://github.com/clojure/core.async),
[reducers](http://clojure.org/reducers), and
[Tesser](https://github.com/aphyr/tesser)).

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
* `doseq`
* `run!`

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

### Unlike the other functions, pdoseq and prun! block

It's worth mentioning that both `pdoseq` and `prun!` block. Since they don't
create a streaming sequence, their work doesn't happen in the "background".
Instead, the calling thread will wait for all the tasks to complete. If you
want to do work without blocking, use something that makes a sequence, like
`upfor`.

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
(a buffer). The buffer is used to help keep the threadpool busy. Unlike core
`pmap`, the buffer size is not fixed at `ncpus + 2`; instead, it defaults to
the size of the threadpool to keep the pool full. Each parallel function also
comes with a -buffer variant that has an extra argument, allowing you to
specify the buffer size.

For instance, these will both cause 10 items to be realized:

```clojure
(require '[com.climate.claypoole :as cp])
(require '[com.climate.claypoole.lazy :as lazy])
(cp/with-shutdown! [pool 2]
  (doall (take 8 (lazy/pmap pool inc (range))))
  (doall (take 4 (lazy/pmap-buffer pool 6 inc (range)))))
```

### Lazy Advantages

The lazy functions work well with sequences too large to fit in memory.
Furthermore, the lazy functions work well with chained maps that operate at
different speeds over large amounts of data. If an eager map that runs quickly
feeds into an eager map that runs slowly, the buffer between them will tend to
grow, possibly running the system out of memory. Lazy functions will avoid this
by only performing work as needed.

As a rule of thumb, if you are working with data too large to fit into memory,
you probably want a lazy operation.

### Lazy Disadvantages

The disadvantage of the lazy functions is that they may not keep the threadpool
fully busy. For instance, this pmap will take 6 seconds to run:

```clojure
(doall (lazy/pmap 2 #(Thread/sleep (* % 1000)) [4 3 2 1]))
```

That's because it will not realize the 2 task until the 4 task is complete, so
one thread in the pool will sit idle for 1 second. On the other hand, an eager
function would only take 5 seconds, since the two threads would be assigned
tasks as follows:

* 4: Thread 0
* 3: Thread 1
* 2: Thread 1
* 1: Thread 0

Note also that an unordered map (`upmap`) would also take 5 seconds here, since
as soon as 3 is complete, it would be returned and the next item would be
forced. In general, to use the threadpool most efficiently with these lazy
functions, prefer the unordered versions.

## How are exceptions handled?

Exceptions are a little tricky in a `pmap` for two reasons.

First, exceptions in `pmaps` are tricky because the task was run in a future,
so Java "helpfully" rethrows the exception as a
`java.util.concurrent.ExecutionException`. Claypoole unwraps that
`ExecutionException` so your code only sees the original exception (as of
Claypoole version 0.4.0). For instance:

```clojure
;; Core map throws the expected NullPointerException.
(try
  (first (map inc [nil]))
  (catch NullPointerException e))
;; Core pmap throws a java.util.concurrent.ExecutionException.
(try
  (first (pmap inc [nil]))
  (catch java.util.concurrent.ExecutionException e))
;; Claypoole pmap throws the expected NullPointerException.
(try
  (first (cp/pmap 2 inc [nil]))
  (catch NullPointerException e))
```

Second, exceptions in `pmaps` are tricky because there are other tasks running,
and it's not quite clear what to do with those. Should they be aborted? Should
they be allowed to continue? How many should be allowed to continue?

Claypoole (as of 0.4.0) works just like core `pmap`: it will not kill queued
tasks, but it will stop adding tasks to the queue. Like core `pmap`, Claypoole's
functions have a buffer of tasks enqueued to keep the thread pool busy. Whereas
core `pmap` has a buffer of ncpus + 2, Claypoole's eager functions use a
buffer size of twice the pool size (to keep the pool busy), and its lazy
functions use a buffer size equal to the pool size (so that they minimize
unneeded work).

```clojure
(let [slow (fn [x] (Thread/sleep 100) x)  ; we slow the work so the buffer fills
      prn+1 (comp prn inc slow)
      data (cons nil (iterate inc 0))]  ; we use iterate inc to avoid chunking
  ;; Core map does no work after an exception, so no numbers will be printed.
  (dorun (map prn+1 data))
  ;; Core pmap does ncpus + 2 work after an exception, so on a quad-core
  ;; computer, 6 numbers will be printed.
  (doall (pmap prn+1 data))
  ;; Claypoole eager pmap does pool size * 2 - 1 work after an exception, since
  ;; the exceptional task is part of the buffer, so 5 numbers will be printed.
  (doall (cp/pmap 3 prn+1 data))
  ;; Claypoole lazy pmap does pool size work after an exception, so 3 numbers
  ;; will be printed.
  (doall (lazy/pmap 3 prn+1 data)))
```

Note that if tasks are still running when the pool is force-shutdown with
`shutdown!`, those tasks will be aborted.

```clojure
(cp/with-shutdown! [pool 2]
  (doall (cp/pmap pool inc (cons nil (range 100)))))
;; Some of those inc tasks will have been aborted because the sequence was
;; truncated by the exception, so the doall finished, and then the pool was
;; immediately shutdown! while a few tasks were still running.
```

(Note: Before Claypoole 0.4.0, its behavior was to kill later tasks if one task
failed. In addition to possibly surprising users, that required more code
complexity than was wise.)

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

You might also like using [dirigiste](https://github.com/ztellman/dirigiste)
threadpools with Claypoole. Dirigiste provides a fast, instrumented
ExecutorService that scales in a controllable way. Note that dirigiste by
default creates thread pools that throw exceptions when full, which makes it
hard for Claypoole to enqueue tasks. Instead, you can use the Executor
constructor directly, though it's a bit ugly:

```clojure
(def pool
  (io.aleph.dirigiste.Executor.
    (java.util.concurrent.Executors/defaultThreadFactory)
    ;; Here's where we insert our non-error queue
    (java.util.concurrent.LinkedBlockingQueue.)
    (io.aleph.dirigiste.Executors/fixedController n-threads)
    n-threads
    ;; Metrics to capture
    (java.util.EnumSet/noneOf io.aleph.dirigiste.Stats$Metric)
    ;; Dirigiste default sample period
    25
    ;; Dirigiste default control period
    10000
    java.util.concurrent.TimeUnit/MILLISECONDS))
```

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
