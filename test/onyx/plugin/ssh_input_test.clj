(ns onyx.plugin.ssh-input-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [taoensso.timbre :refer [info]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.ssh-input]
            [onyx.api]))

(def id (java.util.UUID/randomUUID))

(def env-config 
  {:onyx/tenancy-id id
   :zookeeper/address "127.0.0.1:2188"
   :zookeeper/server? true
   :zookeeper.server/port 2188})

(def peer-config 
  {:onyx/tenancy-id id
   :zookeeper/address "127.0.0.1:2188"
   :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
   :onyx.messaging.aeron/embedded-driver? true
   :onyx.messaging/allow-short-circuit? false
   :onyx.messaging/impl :aeron
   :onyx.messaging/peer-port 40200
   :onyx.messaging/bind-addr "localhost"})

(def n-messages 100)

(def batch-size 20)

(def kill-channel (chan 0))

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.ssh-input/test-input
    :onyx/type :input
    :onyx/medium :ssh
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Documentation for your datasource"
    ;; :ssh-input/target-directories ["/tmp/testsftp"]
    :ssh-input/server-address "localhost"
    :ssh-input/username "hhutch"
    :ssh-input/password "XXXX"
    :ssh-input/test-script [["/tmp/testsftp/bonafide" "/tmp/testsftp/foobar"]]}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Writes segments to a core.async channel"}])

(def workflow [[:in :out]])

(def in-datasource (atom (list)))

(def out-chan (chan (sliding-buffer (inc n-messages))))

(defn inject-in-datasource [event lifecycle]
  {})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-datasource})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.ssh-input/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls ::out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(deftest testing-input
  (def env (onyx.api/start-env env-config))

  (def peer-group (onyx.api/start-peer-group peer-config))


  (doseq [n (range n-messages)]
    (swap! in-datasource conj {:n n}))

  (swap! in-datasource conj :done)

  (def v-peers (onyx.api/start-peers 2 peer-group))

  (onyx.api/submit-job
   peer-config
   {:catalog catalog
    :workflow workflow
    :lifecycles lifecycles
    :task-scheduler :onyx.task-scheduler/balanced})

  (def results (take-segments! out-chan))

  (testing "Input is received at output"
    (let [expected (set (map (fn [x] {:n x}) (range n-messages)))]
      (is (= [] results))
      (is (= :done (last results)))))

  (doseq [v-peer v-peers]
    (onyx.api/shutdown-peer v-peer))

  (onyx.api/shutdown-peer-group peer-group)

  (onyx.api/shutdown-env env))
