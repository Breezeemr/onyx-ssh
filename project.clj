(defproject onyx-ssh "0.8.2.0-SNAPSHOT"
  :description "Onyx plugin for ssh"
  :url "FIX ME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["snapshots" {:url "s3p://breezepackages/snapshots" :creds :gpg}]
                 ["releases" {:url "s3p://breezepackages/releases" :creds :gpg}]]
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [clj-ssh "0.5.14"]
                 [org.onyxplatform/onyx "0.9.6"]
                 [ch.qos.logback/logback-classic "1.1.7"]]
  :profiles {:dev {:dependencies []
                   :plugins []}})
