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

(defproject org.clj-commons/claypoole
  (or (System/getenv "PROJECT_VERSION") "1.2.0")
  :description "Claypoole: Threadpool tools for Clojure."
  :url "https://github.com/clj-commons/claypoole"
  :license {:name "Apache License Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0"
            :distribution :repo}
  :deploy-repositories [["clojars" {:url "https://repo.clojars.org"
                                    :username :env/clojars_username
                                    :password :env/clojars_password
                                    :sign-releases true}]]

  :min-lein-version "2.0.0"
  :source-paths ["src/clj"]
  :resource-paths ["resources"]
  :java-source-paths ["src/java"]
  :pedantic? :warn
  :profiles {:dev {:dependencies [[org.clojure/clojure "1.10.3"]]}
             :clojure-1.9 {:dependencies [[org.clojure/clojure "1.9.0"]]}
             :clojure-1.10 {:dependencies [[org.clojure/clojure "1.10.3"]]}
             :clojure-1.11 {:dependencies [[org.clojure/clojure "1.11.0-rc1"]]}}
  :plugins [[jonase/eastwood "0.2.3"]
            [lein-ancient "0.7.0"]]
  ;; Make sure we build for Java 1.6 for improved backwards compatibility.
  :javac-options ["-target" "1.8" "-source" "1.8"])
