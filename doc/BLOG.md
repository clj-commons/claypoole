# Claypoole: Threadpool tools for Clojure

_Posted on February 25, 2014 by Leon Barrett_

At The Climate Corporation, we have “sprintbaticals”, two-week projects where we can work on something a bit different. This post is about work done by Leon Barrett during his recent sprintbatical.

At the Climate Corporation, we do a lot of resource-intensive scientific modeling, especially of weather and plant growth. We use parallelism, such as pmap, to speed that up whenever possible. We recently released a library, [claypoole](https://github.com/TheClimateCorporation/claypoole), that makes it easy to use and manage threadpools for such parallelism.

To use claypoole, add the Leiningen dependency `[com.climate/claypoole "0.2.1"]`.

# Why?

Basically, we just wanted a better pmap. Clojure’s pmap is pretty awesome, but we wanted to be able to control the number of threads we were using, and it was nice to get a few other bonus features. (Frankly, we were surprised that we couldn’t find such a library when we searched.)

Although the parallelism we need is simple, the structure of our computations is often relatively complex. We first compute some things, then make requests to a service, then process some other stuff, then … you get the picture. We want to be able to control our number of threads across multiple stages of work and multiple simultaneous requests.

Nevertheless, we don’t really need [core.async’s asynchronous programming](https://github.com/clojure/core.async). Coroutines and channels are nice, but our parallelism needs don’t require their complexity, and we’d still have to manage the amount of concurrency we were using.

Similarly, [reducers](http://clojure.org/reducers) are great, but they’re really just oriented at CPU-bound tasks. We needed more flexibility than that.

# Aside: So why do you need so many threads?

Like many of you, we’re consuming resources that have some ideal amount of parallelism: they have some maximum throughput, and trying to use more or less than that is ineffective. For instance, we want to use our CPU cores but not have too many context switches, and we want to amortize our network latency but not overload our backend services.

Consider using parallelism to amortize network latency. Each request we make has a delay (latency) before the server begins responding, plus a span of network transfer. If we just run serial network requests, we’ll see a timeline like this:

![serial](serial1.png)

That means that we’re not actually making good use of our network bandwidth. In fact, the network is sitting idle for most of the time. Instead, with optimal parallelism, we’ll get much fuller usage of our bandwidth by having the latency period of the requests overlap.

![parallel](parallel2.png)

The transfers may be individually somewhat slower because we’re sharing bandwidth, but on average we finish sooner. On the other hand, with too much parallelism, we’ll use our bandwidth well, but we’ll see our average total latency go up:

![over-parallel](over-parallel2.png)

That’s why we want to be able to control how much parallelism we use.

# How do I use it?

Just make a threadpool and use it in claypoole’s version of a parallel function like future, pmap, pcalls, and so on. We even made a parallel for.

```clojure
(require '[com.climate.claypoole :as cp])
(cp/with-shutdown! [pool (cp/threadpool 4)]
  (cp/future pool (+ 1 2))
  (cp/pmap pool inc (range 10))
  (cp/pvalues pool (str "si" "mul") (str "ta" "neous"))
  (cp/pfor pool [i (range 10)]
    (* i (- i 2))))
```

They stream their results eagerly, so you don’t have to force them to be realized with something like doall as you would for .core.pmap. And, because they produce sequential streams of output and take sequential streams of input, you can chain them easily.

```clojure
(->> (range 3)
     (cp/pmap pool inc)
     (cp/pmap pool #(* 2 %))
     (cp/pmap other-pool #(doto % log/info)))
```

# Got anything cooler than that?

You don’t have to manage the threadpool at all, really. If you just need a temporary pool (and don’t care about the overhead of spawning new threads), you can just let the parallel function do it for you.

```clojure
;; Instead of a threadpool, we just pass a number of threads (4).
(cp/pmap 4 inc (range 4))
```

To reduce latency, you can use unordered versions of these functions that return results in the order they’re completed.

```clojure
;; This will probably return '(0 1 2), depending on how
;; the OS schedules our threads.
(cp/upfor 3 [i (reverse (range 3))]
  (do
    (Thread/sleep (* i 1000))
    (inc i)))
```

For instance, if we’re fetching and resizing images from the network, some images might be smaller and download faster, so we can start resizing them first.

```clojure
(->> image-urls
     ;; Put the URL in a map.
     (map (fn [url] {:url url}))
     ;; Add the image data to the map.
     (cp/upmap network-pool
               #(assoc % :data
                       (-> % :url clj-http.client/get :body)))
     ;; Add the resized image to the map.
     (cp/upmap cpu-pool
               #(assoc % :resized (resize (:data %)))))
```

You can also have your tasks run in priority order. Tasks are chosen as threads become available, so the highest-priority task at any moment is chosen. (So, for instance, the first task submitted to a pool will run first, regardless of priority.)

```clojure
(require '[com.climate.claypoole.priority :as cpp])
(cp/with-shutdown! [pool (cpp/priority-threadpool 4)]
  (let [;; These will mostly run last.
        xs (cp/pmap (cpp/with-priority pool 0) inc (range 10))
        ;; These will mostly run first.
        ys (cp/pmap (cpp/with-priority pool 10) dec (range 10))]
    ...))
```

# What’s next?

We don’t have particularly specific plans at this time. There are a number of interesting tricks to play with threadpools and parallelism. For instance, tools for [ForkJoinPools](http://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ForkJoinPool.html) could combine this work with [reducers](http://clojure.org/reducers), support for web workers in Clojurescript would be nice, and there are many other such opportunities.

Send us your requests (and pull requests) on [Github](https://github.com/TheClimateCorporation/claypoole)!

# Where can I learn more?

A detailed README can be seen on the [claypoole Github project](https://github.com/TheClimateCorporation/claypoole).

# Thanks!

Thanks to Sebastian Galkin of Climate Corp. and to Jason Wolfe of [Prismatic](http://getprismatic.com/), who helped with advice on API design decisions.
