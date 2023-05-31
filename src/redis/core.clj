(ns redis.core
  (:require [clojure.java.io :as io]
            [clojure.string :as str])
  (:import [java.net ServerSocket])
  (:gen-class))


(defn receive-message
  "Read all lines of textual data from the given socket"
  [socket]
  (let [reader (io/reader socket)
        first-line (.readLine reader)]
    (when first-line
      (loop [line first-line
             res []]
        (if (.ready reader)
          (recur (.readLine reader) (conj res line))
          (conj res line))))))

(defn send-message
  "Send the given string message out over the given socket"
  [socket msg]
  (let [writer (io/writer socket)]
    (.write writer msg)
    (.flush writer)))

(defn handle-client [sock handler]
  (doseq [msg-in (repeatedly #(receive-message sock))
          :while msg-in
          :let [msg-out (handler msg-in)]]
    (send-message sock msg-out)))

(defn serve [port handler]
  (with-open [server-sock (ServerSocket. port)]
    (. server-sock (setReuseAddress true))
    (while true
      (let [sock (.accept server-sock)]
        (future
          (handle-client sock handler))))))

(defn reply [str]
  (str/join ["+" str "\r\n"]))

(def database (atom {}))

(defmulti handle-command (fn [[command & _]]
                           (keyword (str/lower-case command))))

(defmethod handle-command :ping
  [_]
  (reply "PONG"))

(defmethod handle-command :echo
  [[_ [arg-len arg]]]
  (reply arg))

(defmethod handle-command :set
  [[_ [key-len key val-len val opt-len opt optarg-len optarg]]]
  (swap! database assoc key val)
  (when (and opt (= (.toUpperCase opt) "PX"))
    (let [timeout (Integer/parseInt optarg)]
      (future
        (Thread/sleep timeout)
        (swap! database assoc key nil))))
  (reply "OK"))

(defmethod handle-command :get
  [[_ [key-len key]]]
  (if-let [value (get @database key "(nil)")]
    (reply value)
    "$-1\r\n"))

;; needed for redis-cli
(defmethod handle-command :command
  [_]
  (reply "O hai"))

(defn handler [message]
  (let [[array-len command-len command & args] message]
    (handle-command [command args])))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (serve 6379 handler))
