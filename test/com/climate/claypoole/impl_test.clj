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


(deftest test-map-indexed-with-rest
  (is (= (impl/map-indexed-with-rest vector [:x :y :z])
         '([0 :x ([1 :y ([2 :z ()])]
                  [2 :z ()])]
           [1 :y ([2 :z ()])]
           [2 :z ()]))))
