(ns onyx.plugin.ssh-output.lifecycles)

(def out-datasink (atom []))

(defn inject-out-datasink [event lifecycle]
  {:ssh/ssh-datasink out-datasink})

(def sftp-out-calls
  {:lifecycle/before-task-start inject-out-datasink})
