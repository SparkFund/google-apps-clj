(ns sparkfund.google.client
  (:require [clojure.core.async :as async]
            [clojure.java.io :as io])
  (:import
   [com.google.api.client.googleapis.auth.oauth2 GoogleCredential]
   [com.google.api.client.googleapis.javanet GoogleNetHttpTransport]
   [com.google.api.client.googleapis.services AbstractGoogleClientRequest]
   [com.google.api.client.http
    HttpRequest
    HttpRequestInitializer
    HttpTransport]
   [com.google.api.client.json JsonFactory]
   [com.google.api.client.json.jackson2 JacksonFactory]
   [java.time Duration]))

(def ^HttpTransport http-transport
  (GoogleNetHttpTransport/newTrustedTransport))

(def ^JsonFactory json-factory
  (JacksonFactory/getDefaultInstance))

(def default-request-initializer-config
  {::read-timeout (Duration/ofMinutes 5)
   ::connect-timeout (Duration/ofMinutes 5)})

;; TODO exponential backoff for request failures could be built
;; via com.google.api.client.util.ExponentialBackOff

(defn build-request-initializer
  [config]
  (let [config (merge default-request-initializer-config config)
        {::keys [credentials scopes read-timeout connect-timeout]} config
        request-initializer
        (with-open [input (io/input-stream credentials)]
          (-> (GoogleCredential/fromStream input)
              (.createScoped scopes)))]
    (reify HttpRequestInitializer
      (^void initialize [_ ^HttpRequest request]
       (.initialize ^HttpRequestInitializer request-initializer request)
       (.setConnectTimeout request (.toMillis ^Duration connect-timeout))
       (.setReadTimeout request (.toMillis ^Duration read-timeout))))))

(def default-executor-config
  {::concurrency 2
   ::shutdown-duration (Duration/ofSeconds 20)})

(defn build-executor
  "Builds an executor that execute google client requests concurrently,
   with the maximum number of threads controlled by the ::concurrency
   config value.

   This returns a map with an ::input channel and a ::close! fn.

   The input channel accepts maps which must contain a google client
   request in the ::request key and an ::output channel. The executor
   executes the request. If the request was processed successfully, the
   executor adds ::success? true and the ::response to the map and writes
   it to the output channel. If the execution throws an exception, the
   executor adds ::success? false and the ::exception to the map and
   writes it to the output channel.

   When the close! fn is called, the executor closes the input channel
   and gives any extant workers as long as the ::shutdown-duration in the
   config to finish before writing failures to their output channels and
   shutting down."
  [config]
  (let [config (merge default-executor-config config)
        {::keys [concurrency input ^Duration shutdown-duration]} config
        control (async/chan)
        machine (async/go-loop [workers {}
                                shutdown nil]
                  ;; If we shutting down and all work is done, we don't
                  ;; need to wait for the shutdown channel to timeout
                  (when (and shutdown (not (seq workers)))
                    (async/close! shutdown))
                  (let [chans (cond-> (into [] (keys workers))
                                shutdown
                                (conj shutdown)
                                (not shutdown)
                                (conj control)
                                (< (count workers) concurrency)
                                (conj input))
                        [value chan] (async/alts! chans)]
                    (condp = chan
                      input
                      (when (some? value)
                        (let [{::keys [^AbstractGoogleClientRequest request output]} value
                              worker (async/thread
                                       (let [response (try
                                                        (let [response (.execute request)]
                                                          (assoc value
                                                                 ::success? true
                                                                 ::response response))
                                                        (catch Exception e
                                                          (assoc value
                                                                 ::success? false
                                                                 ::exception e
                                                                 ::event :execute-exception)))]
                                         (async/put! output response)))]
                          (recur (assoc workers worker value) shutdown)))

                      control
                      (recur workers (async/timeout (.toMillis shutdown-duration)))

                      shutdown
                      (doseq [value (vals workers)]
                        (let [{::keys [output]} value]
                          (async/>! output (assoc value
                                                  ::success? false
                                                  ::event ::shutdown))))

                      ;; must be a worker
                      (recur (dissoc workers chan) shutdown))))]
    {::close! (fn []
                (async/close! control)
                (async/<!! machine))}))

(defn build-client
  [config]
  (let [input (async/chan)]
    {::input input
     ::executor (build-executor (assoc config ::input input))}))

(defn stop-client
  [client]
  (let [{::keys [input executor]} client]
    (async/close! input)
    ((get executor ::close!))))

(defn execute-requests
  [client requests]
  (let [{::keys [input]} client]
    (async/go
      (let [outputs (into []
                          (map (fn [request]
                                 (let [output (async/promise-chan)
                                       value {::request request
                                              ::output output}]
                                   (when-not (async/put! input value)
                                     (async/put! output {::success? false
                                                         ::event ::input-closed}))
                                   output)))
                          requests)
            responses (transient [])]
        (doseq [output outputs]
          (conj! responses (async/<! output)))
        (persistent! responses)))))
