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

(defmulti handle-command (fn [[command & _]]
                           (keyword (str/lower-case command))))

(defmethod handle-command :ping
  [_]
  "+PONG\r\n")

(defmethod handle-command :echo
  [[_ [arg-len arg]]]
  (str/join ["+" arg "\r\n"]))

;; needed for redis-cli
(defmethod handle-command :command
  [_]
  "+PONG\r\n")

(defn handler [message]
  (let [[array-len command-len command & args] message]
    (handle-command [command args])))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (serve 6379 handler))
