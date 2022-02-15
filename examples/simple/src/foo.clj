(ns foo
  (:require [com.climate.claypoole :as cp]
            [com.climate.claypoole.lazy :as lazy]))

(let [slow (fn [x] (Thread/sleep 100) x)  ; we slow the work so the buffer fills
      prn+1 (comp prn inc slow)
      data (cons 1 (take 10 (iterate inc 0)))]  ; we use iterate inc to avoid chunking
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


(comment

  (println "x")

  0)