(ns onyx.plugin.ssh-output-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is testing]]
            [taoensso.timbre :refer [info]]
            [onyx.plugin.core-async]
            [onyx.plugin.ssh-output]
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

(def n-messages 5)

(def batch-size 20)

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.ssh-output/sftp-output
    :onyx/type :output
    :onyx/medium :ssh
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Documentation for your datasink"
    :ssh-output/server-address "localhost"
    :ssh-output/username "hhutch"
    :ssh-output/password "XXXXX"}])

(def workflow [[:in :out]])

(def in-chan (chan (inc n-messages)))

(def out-datasink (atom []))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(defn inject-out-datasink [event lifecycle]
  {:ssh/ssh-datasink out-datasink})

(def out-calls
  {:lifecycle/before-task-start inject-out-datasink})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls ::out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.ssh-output/writer-calls}])



(deftest testing-output
  (def my-messages (mapv
                    #(let [tmpfile1 (java.io.File/createTempFile "onyx-ssh" (str "test-" %))]
                       {:payload (.getBytes (format "at %s TEST: %d" (java.util.Date.) %))
                        :filename (.getPath tmpfile1)
                        :fileobj tmpfile1})
                    (range n-messages)))
  
  (def env (onyx.api/start-env env-config))

  (def peer-group (onyx.api/start-peer-group peer-config))

  ;; (>!! in-chan my-message)

  (doseq [n (range n-messages)]
    (>!! in-chan (get my-messages n)))

  (>!! in-chan :done)
  (close! in-chan)

  (def v-peers (onyx.api/start-peers 2 peer-group))

  (def job-info 
    (onyx.api/submit-job
     peer-config
     {:catalog catalog
      :workflow workflow
      :lifecycles lifecycles
      :task-scheduler :onyx.task-scheduler/balanced}))

  (info "Awaiting job completion")

  (onyx.api/await-job-completion peer-config (:job-id job-info))

  (def results @out-datasink)

  (testing "Output is written correctly"
    (let [expected (set (map (fn [x] {:n x}) (range n-messages)))]
      (is (= (->> my-messages (into #{}) (map #(String. (:payload %))) (into #{}))
             (->> results butlast set (map #(String. (:payload %))) (into #{}))))
      (is (= :done (last results)))))


  (doseq [v-peer v-peers]
    (onyx.api/shutdown-peer v-peer))

  (onyx.api/shutdown-peer-group peer-group)

  (onyx.api/shutdown-env env)

  (doseq [m my-messages]
    (.delete (:fileobj m))))
