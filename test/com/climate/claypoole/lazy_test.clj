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
    [com.climate.claypoole.lazy :as lazy]
    [com.climate.claypoole-test :as cptest]))


;; TODO add tests for laziness

(def check-all cptest/check-all)

(deftest test-pmap
  (check-all "pmap" lazy/pmap true true true))

(deftest test-upmap
  (check-all "upmap" lazy/upmap false true true))

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
    (check-all "pcalls" pmap-like true true true)))

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
    (check-all "upcalls" pmap-like false true true)))

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
    (check-all "pvalues" pmap-like true false true)))

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
    (check-all "upvalues" pmap-like false false true)))

(deftest test-pfor
  (testing "basic pfor test"
    (is (= (range 1 11)
           (lazy/pfor 3 [i (range 10)] (inc i)))))
  (letfn [(pmap-like [pool work input]
            (lazy/pfor
              pool
              [i input]
              (work i)))]
    (check-all "pfor" pmap-like true true true)))

(deftest test-upfor
  (testing "basic upfor test"
    (is (= (range 1 11)
           (sort (lazy/pfor 3 [i (range 10)] (inc i))))))
  (letfn [(pmap-like [pool work input]
            (lazy/upfor
              pool
              [i input]
              (work i)))]
    (check-all "upfor" pmap-like false true true)))
