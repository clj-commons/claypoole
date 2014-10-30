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

(ns com.climate.claypoole.impl-test
  (:require
    [clojure.test :refer :all]
    [com.climate.claypoole.impl :as impl]))


(deftest test-queue-seq
  (let [[q qs] (impl/queue-seq)]
    (doseq [i (range 10)]
      (impl/queue-seq-add! q i))
    (impl/queue-seq-end! q)
    (is (= (range 10) qs))))

(deftest test-lazy-co-read
  (let [s1 (range 10)
        s2 (concat (range 10) (lazy-seq (deref (promise))))]
    (is (= (map #(list % %) (range 10))
           (impl/lazy-co-read s1 s2)))))

(deftest test-subseqs
  (let [n 10]
    (is (= (impl/subseqs (range n))
           (map #(drop % (range n)) (range (inc n)))))))
