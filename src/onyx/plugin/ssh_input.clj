(ns onyx.plugin.ssh-input
  (:require [onyx.peer.function :as function]
            [onyx.peer.pipeline-extensions :as p-ext]
            [onyx.static.default-vals :refer [defaults arg-or-default]]
            [onyx.types :as t]
            [taoensso.timbre :refer [debug info] :as timbre]
            [clojure.java.io :refer [input-stream output-stream]]
            [clj-ssh.ssh :as ssh]
            [clojure.core.async :refer [thread chan alts!! alts! poll! >!! <!! close!]]))

;; Often you will need some data in your event map for use by the plugin 
;; or other lifecycle functions. Try to place these in your builder function (pipeline)
;; first if possible.
(defn inject-into-eventmap
  [event lifecycle]
  (let [pipeline (:onyx.core/pipeline event)] 
    {:ssh/pending-messages (:pending-messages pipeline) 
     :ssh/drained? (:drained? pipeline)}))

(def reader-calls 
  {:lifecycle/before-task-start inject-into-eventmap})

(defn input-drained? [pending-messages batch]
  (and (= 1 (count @pending-messages))
       (= (count batch) 1)
       (= (:message (first batch)) :done)))

(defrecord SFTPFetch [max-pending batch-size batch-timeout
                      seen-files pending-files drained?
                      kill-channel get-files]
  p-ext/Pipeline
  ;; Write batch can generally be left as is. It simply takes care of
  ;; Transmitting segments to the next task
  (write-batch 
    [this event]
    (function/write-batch event))

  (read-batch [_ {:keys [onyx.core/task-map] :as event}]
    (when-let [[value f-chan] (alts!! [get-files kill-channel])]
      (if (= f-chan kill-channel)
        (let [tmp-uuid (java.util.UUID/randomUUID)]
          (reset! drained? true)
          (swap! pending-files conj tmp-uuid)
          {:onyx.core/batch [(t/input tmp-uuid :done)]})
        (let [files (into [] value)
              {:keys [ssh-input/username ssh-input/password ssh-input/server-address
                      ssh-input/target-directories]} task-map
              ;; max-segments (min (- max-pending pending) batch-size)
              ;; Read a batch of up to batch-size from your data-source
              ;; For data-sources which enable read timeouts, please
              ;; make sure to pass batch-timeout into the read call
              s-agent (ssh/ssh-agent {:use-system-ssh-agent false})
              s-session (ssh/session s-agent server-address {:username username
                                                             :password password})
              my-tmpfl (java.io.File/createTempFile "onyx-ssh" ".test-00")
              batch (ssh/with-connection s-session
                     (mapv (fn [f]
                             (let [my-uuid (java.util.UUID/randomUUID)
                                   my-tmpfl (java.io.File/createTempFile "onyx-ssh" (str ".test-" my-uuid))
                                   s-channel (ssh/ssh-sftp s-session)]
                               (ssh/ssh-sftp-cmd s-channel :get [f (.getPath my-tmpfl)] {})
                               (ssh/disconnect-channel s-channel)
                               (t/input my-uuid {:id my-uuid
                                                 :payload (slurp (.getPath my-tmpfl))
                                                 :filename f
                                                 :tmp-filename (.getPath my-tmpfl)
                                                 :fileobj my-tmpfl}))) 
                           (take batch-size files)))]
          (ssh/disconnect s-session)
          
          ;; Add the read batch to your pending-messages so they can be 
          ;; retried later if necessary
          (doseq [m batch]
            (swap! pending-files conj (:id m)))
          ;; Check if you've seen the :done and all the messages have been consumed
          ;; If so, set the drained? atom, which will be returned by drained?
          {:onyx.core/batch batch}))))
  
  (seal-resource [this event]
    ;; Nothing is required here, however generally most plugins 
    ;; have resources (e.g. a connection) to clean up
    )

  p-ext/PipelineInput
  (ack-segment [_ _ segment-id]
    ;; When a message is fully acked you can remove it from pending-messages
    ;; Generally this can be left as is.

    ;; delete from labcorp server
    (swap! pending-files disj segment-id))

  (retry-segment 
    [_ {:keys [#_ssh/ssh-datasource] :as event} segment-id]
    ;; Messages are retried if they are not acked in time
    ;; or if a message is forcibly retried by flow conditions.
    ;; Generally this takes place in two steps
    ;; Take the message out of your pending-messages atom, and put it 
    ;; back into a datasource or a buffer that are you are reading into
    (when-let [msg (get @pending-files segment-id)]
      (swap! pending-files disj segment-id)))

  (pending?
    [_ _ segment-id]
    ;; Lookup a message in your pending messages map.
    ;; Generally this can be left as is
    (get @pending-files segment-id))

  (drained? 
    [_ _]
    ;; Return whether the input has been drained. This is set in the read-batch
    @drained?)) 

;; Builder function for your plugin. Instantiates a record.
;; It is highly recommended you inject and pre-calculate frequently used data 
;; from your task-map here, in order to improve the performance of your plugin
;; Extending the function below is likely good for most use cases.
(defn input [event]
  (let [task-map (:onyx.core/task-map event)
        {:keys [ssh-input/username ssh-input/password ssh-input/server-address ssh-input/target-directories]} task-map
        max-pending (arg-or-default :onyx/max-pending task-map)
        batch-size (:onyx/batch-size task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        seen-files (atom #{})
        pending-files (atom #{})
        drained? (atom false)
        s-agent (ssh/ssh-agent {:use-system-ssh-agent false})
        
        get-files (chan 0)
        kill-channel (chan 0)]
    (thread (loop []
              (when-not (poll! kill-channel)
                (let [s-session (ssh/session s-agent server-address {:username username
                                                                     :password password})]
                  (ssh/with-connection s-session
                    (let [channel (ssh/ssh-sftp s-session)
                          ls-out (flatten (mapv (fn [d]
                                                  (->> (ssh/ssh-sftp-cmd channel :ls [d] {})
                                                       (filter #(not (or (= "." (.getFilename %))
                                                                         (= ".." (.getFilename %)))))
                                                       (mapv #(str (clojure.string/replace d  #"([^/])$" "$1/") (.getFilename %)))))
                                                target-directories))
                          unseen-items (remove @seen-files ls-out)]
                      
                      ;; put on @pending-files, if not already in it, and add to get-files channel.. control point for checking is here
                      (>!! get-files unseen-items)
                      (swap! seen-files into unseen-items)
                      (ssh/disconnect-channel channel)))
                  (ssh/disconnect s-session))                
                (Thread/sleep 100)
                (recur))))
    (->SFTPFetch max-pending batch-size batch-timeout seen-files pending-files drained? kill-channel get-files)))

(defn test-input [event]
  (let [task-map (:onyx.core/task-map event)
        {:keys [ssh-input/username ssh-input/password ssh-input/server-address
                ssh-input/target-directories ssh-input/test-script]} task-map
        max-pending (arg-or-default :onyx/max-pending task-map)
        batch-size (:onyx/batch-size task-map)
        batch-timeout (arg-or-default :onyx/batch-timeout task-map)
        seen-files (atom #{})
        pending-files (atom #{})
        drained? (atom false)
        s-agent (ssh/ssh-agent {:use-system-ssh-agent false})
        
        get-files (chan 0)
        kill-channel (chan 0)]
    (thread
      (doseq [t test-script]
        (>!! get-files t))
      (Thread/sleep 2000)
      (>!! kill-channel 1))
    (->SFTPFetch max-pending batch-size batch-timeout seen-files pending-files drained? kill-channel get-files)))
