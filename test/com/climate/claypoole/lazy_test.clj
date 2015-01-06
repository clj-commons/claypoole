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

(ns com.climate.claypoole.lazy-test
  (:require
    [clojure.test :refer :all]
    [com.climate.claypoole :as cp]
    [com.climate.claypoole.impl :as impl]
    [com.climate.claypoole.lazy :as lazy]
    [com.climate.claypoole-test :as cptest]))


(deftest test-seq-open
  (testing "seq-open doesn't call f early"
    (let [a (atom false)]
      (->> (range 10)
           (#'lazy/seq-open #(reset! a true))
           (take 5)
           doall)
      (is (false? @a))))
  (testing "seq-open calls f when s is complete"
    (let [a (atom false)]
      (->> (range 10)
           (#'lazy/seq-open #(reset! a true))
           doall)
      (is (true? @a))))
  (testing "seq-open calls f when there's an exception"
    (let [a (atom false)]
      (is (thrown? ClassCastException
                   (->> [1 :x 2]
                        impl/unchunk
                        (map inc)
                        (#'lazy/seq-open #(reset! a true))
                        doall)))
      (is (true? @a)))))

(defn check-input-laziness
  "Check that a function is actually lazy in reading its input."
  [pmap-like]
  (let [started (atom #{})
        readahead 10
        input (->> (* 3 readahead)
                   range
                   impl/unchunk
                   (map #(do (swap! started conj %) %)))]
    (doall (take 2 (pmap-like readahead inc input)))
    (Thread/sleep 10)
    ;; Exactly what we asked for, plus readahead, is realized.
    (is (= (+ 2 readahead) (count @started)))))

(defn check-output-laziness
  "Check that a function is actually lazy in computing its output."
  [pmap-like]
  (let [started (atom #{})
        readahead 10
        input (range (* 3 readahead))]
    (doall (take 2 (pmap-like readahead #(do (swap! started conj %) %) input)))
    (Thread/sleep 10)
    ;; Exactly what we asked for, plus readahead, is realized.
    (is (= (+ 2 readahead) (count @started)))))

(defn check-all
  [fn-name pmap-like ordered? streaming?]
  ;; Apply all the standard tests
  (cptest/check-all fn-name pmap-like ordered? streaming? true)
  (when streaming?
    (testing (format "%s is lazy in its input" fn-name)
      (check-input-laziness pmap-like)))
  (testing (format "%s is lazy in its output" fn-name)
    (check-output-laziness pmap-like)))

(defn check-input-controllable-readahead
  "Check that the manual pmap function's readahead for the input obeys the
  parameter."
  [manual-pmap-like]
  (let [started (atom #{})
        readahead 10
        input (->> (* 3 readahead)
                   range
                   impl/unchunk
                   (map #(do (swap! started conj %) %)))]
    (doall (take 2 (manual-pmap-like 3 readahead inc input)))
    (Thread/sleep 10)
    ;; Exactly what we asked for, plus readahead, is realized.
    (is (= (+ 2 readahead) (count @started)))))

(defn check-output-controllable-readahead
  "Check that the manual pmap function's readahead for the output obeys the
  parameter."
  [manual-pmap-like]
  (let [started (atom #{})
        readahead 10
        input (range (* 3 readahead))]
    (doall (take 2 (manual-pmap-like 3 readahead #(do (swap! started conj %) %) input)))
    (Thread/sleep 10)
    ;; Exactly what we asked for, plus readahead, is realized.
    (is (= (+ 2 readahead) (count @started)))))

(defn check-all-buffer
  [fn-name manual-pmap-like ordered? streaming?]
  (check-all fn-name (fn [p f i] (manual-pmap-like p 10 f i))
             ordered? streaming?)
  (when streaming?
    (testing (format "%s is lazy in its input" fn-name)
      (check-input-controllable-readahead manual-pmap-like)))
  (testing (format "%s is lazy in its output" fn-name)
    (check-output-controllable-readahead manual-pmap-like)))

(deftest test-pmap
  (check-all "pmap" lazy/pmap true true))

(deftest test-pmap-buffer
  (check-all-buffer "pmap-buffer" lazy/pmap-buffer true true))

(deftest test-upmap
  (check-all "upmap" lazy/upmap false true))

(deftest test-upmap-buffer
  (check-all-buffer "upmap-buffer" lazy/upmap-buffer false true))

(deftest test-pcalls
  (testing "basic pcalls test"
    (is (= [1 2 3 4]
           (lazy/pcalls 3 #(inc 0) #(inc 1) #(inc 2) #(inc 3)))))
  (letfn [(pmap-like [pool work input]
            (apply
              lazy/pcalls
              pool
              (for [i input]
                #(work i))))]
    (check-all "pcalls" pmap-like true true)))

(deftest test-pcalls-buffer
  (letfn [(pmap-like [pool buffer work input]
            (apply
              lazy/pcalls-buffer
              pool buffer
              (for [i input]
                #(work i))))]
    (check-all-buffer "pcalls-buffer" pmap-like true true)))

(deftest test-upcalls
  (testing "basic pcalls test"
    (is (= [1 2 3 4]
             (sort (lazy/upcalls 3 #(inc 0) #(inc 1) #(inc 2) #(inc 3))))))
  (letfn [(pmap-like [pool work input]
            (apply
              lazy/upcalls
              pool
              (for [i input]
                #(work i))))]
    (check-all "upcalls" pmap-like false true)))

(deftest test-upcalls-buffer
  (letfn [(pmap-like [pool buffer work input]
            (apply
              lazy/upcalls-buffer
              pool buffer
              (for [i input]
                #(work i))))]
    (check-all-buffer "upcalls-buffer" pmap-like false true)))

(deftest test-pvalues
  (testing "basic pvalues test"
    (is (= [1 2 3 4]
           (lazy/pvalues 3 (inc 0) (inc 1) (inc 2) (inc 3)))))
  (letfn [(pmap-like [pool work input]
            (let [worksym (gensym "work")]
              ((eval
                 `(fn [pool# ~worksym]
                    (lazy/pvalues
                      pool#
                      ~@(for [i input]
                          (list worksym i)))))
                 pool work)))]
    (check-all "pvalues" pmap-like true false)))

(deftest test-pvalues-buffer
  (letfn [(pmap-like [pool buffer work input]
            (let [worksym (gensym "work")]
              ((eval
                 `(fn [pool# buffer# ~worksym]
                    (lazy/pvalues-buffer
                      pool# buffer#
                      ~@(for [i input]
                          (list worksym i)))))
                 pool buffer work)))]
    (check-all-buffer "pvalues-buffer" pmap-like true false)))

(deftest test-upvalues
  (testing "basic upvalues test"
    (is (= [1 2 3 4]
           (sort (lazy/upvalues 3 (inc 0) (inc 1) (inc 2) (inc 3))))))
  (letfn [(pmap-like [pool work input]
            (let [worksym (gensym "work")]
              ((eval
                 `(fn [pool# ~worksym]
                    (lazy/upvalues
                      pool#
                      ~@(for [i input]
                          (list worksym i)))))
                 pool work)))]
    (check-all "upvalues" pmap-like false false)))

(deftest test-upvalues-buffer
  (letfn [(pmap-like [pool buffer work input]
            (let [worksym (gensym "work")]
              ((eval
                 `(fn [pool# buffer# ~worksym]
                    (lazy/upvalues-buffer
                      pool# buffer#
                      ~@(for [i input]
                          (list worksym i)))))
                 pool buffer work)))]
    (check-all-buffer "upvalues-buffer" pmap-like false false)))

(deftest test-pfor
  (testing "basic pfor test"
    (is (= (range 1 11)
           (lazy/pfor 3 [i (range 10)] (inc i)))))
  (letfn [(pmap-like [pool work input]
            (lazy/pfor
              pool
              [i input]
              (work i)))]
    (check-all "pfor" pmap-like true true)))

(deftest test-pfor-buffer
  (letfn [(pmap-like [pool buffer work input]
            (lazy/pfor-buffer
              pool buffer
              [i input]
              (work i)))]
    (check-all-buffer "pfor-buffer" pmap-like true true)))

(deftest test-upfor
  (testing "basic upfor test"
    (is (= (range 1 11)
           (sort (lazy/pfor 3 [i (range 10)] (inc i))))))
  (letfn [(pmap-like [pool work input]
            (lazy/upfor
              pool
              [i input]
              (work i)))]
    (check-all "upfor" pmap-like false true)))

(deftest test-upfor-buffer
  (letfn [(pmap-like [pool buffer work input]
            (lazy/upfor-buffer
              pool buffer
              [i input]
              (work i)))]
    (check-all-buffer "upfor-buffer" pmap-like false true)))
