(ns sparkfund.google.sheets
  (:require [clojure.core.async :as async]
            [google-apps-clj.credentials :as credentials])
  (:import [com.google.api.client.googleapis.services
            AbstractGoogleClientRequest]
           [com.google.api.client.http
            HttpRequest
            HttpRequestInitializer]
           [com.google.api.services.sheets.v4
            Sheets
            SheetsScopes
            Sheets$Builder]
           [com.google.api.services.sheets.v4.model
            BatchUpdateSpreadsheetRequest
            GridCoordinate
            GridProperties
            Request
            SheetProperties
            UpdateCellsRequest
            UpdateSheetPropertiesRequest]
           [java.time Duration]))

(def default-request-initializer-config
  {::read-timeout (Duration/ofMinutes 5)
   ::connect-timeout (Duration/ofMinutes 5)})

(def scopes
  [SheetsScopes/SPREADSHEETS])

(defn build-request-initializer
  [config]
  (let [config (merge default-request-initializer-config config)
        {::keys [service read-timeout connect-timeout]} config
        request-initializer
        (let [creds (credentials/credential-from-json-stream service)]
          (credentials/credential-with-scopes creds scopes))]
    (reify HttpRequestInitializer
      (^void initialize [_ ^HttpRequest request]
       (.initialize ^HttpRequestInitializer request-initializer request)
       (.setConnectTimeout request (.toMillis ^Duration connect-timeout))
       (.setReadTimeout request (.toMillis ^Duration read-timeout))))))

;; TODO exponential backoff for request failures could be built into the executor
;; or it could be handled by the underlying Java library
;; via com.google.api.client.util.ExponentialBackOff
;; The former allows us more control and could allow us to continue to use
;; worker threads while in the backoff period.
;; The latter makes it Someone Else's Problem and implicitly throttles requests
;; the the service while it's sad.

(defn build-service
  [config]
  (let [{::keys [app]} config
        request-initializer (build-request-initializer config)]
    (.build (doto (Sheets$Builder. credentials/http-transport
                                   credentials/json-factory
                                   request-initializer)
              (.setApplicationName app)))))

(def default-executor-config
  {::concurrency 2
   ::shutdown-duration (Duration/ofSeconds 20)})

(defn build-executor
  "Builds an executor that execute google client requests concurrently.

   This returns a map with an ::input channel, a ::control channel,
   and an ::output channel.

   The input channel accepts maps which must contain a google client
   request in the ::request key and an ::output channel. The executor
   executes the request. If the request was processed successfully, the
   executor adds ::status true and the ::response to the map and writes
   it to the output channel. If the execution throws an exception, the
   executor adds ::status false and the ::exception to the map and
   writes it to the output channel.

   The executor runs with the number of threads given by
   the ::concurrency value in the config map. When the control channel
   closes, this closes the input channel and gives any active
   workers ::shutdown-duration in which to finish before
   adding ::status false ::event ::shutdown to the request map and
   writing to the output channels. The executor will be fully shutdown
   when the top-level ::output channel closes."
  [config]
  (let [config (merge default-executor-config config)
        {::keys [concurrency ^Duration shutdown-duration]} config
        control (async/chan)
        input (async/chan)
        machine (async/go-loop [workers {}
                                shutdown nil]
                  (let [chans (cond-> (into [] (keys workers))
                                shutdown
                                (conj shutdown)
                                (and (not shutdown) (< (count workers) concurrency))
                                (conj input))
                        [value chan] (async/alts! chans)]
                    (condp = chan
                      input
                      (do
                        (println "input" value)
                        (let [{::keys [^AbstractGoogleClientRequest request output]} value
                              worker (async/thread
                                       (let [response (try
                                                        (let [response (.execute request)]
                                                          (assoc value
                                                                 ::status true
                                                                 ::response response))
                                                        (catch Exception e
                                                          (assoc value
                                                                 ::status false
                                                                 ::exception e
                                                                 ::event :execute-exception)))]
                                         (async/put! output response)))]
                           (recur (assoc workers worker value) shutdown)))

                      control
                      (do
                        (println "control")
                        (async/close! input)
                        (recur workers (async/timeout (.toMillis shutdown-duration))))

                      ;; TODO this will always wait for shutdown ms, but we
                      ;; should stop as soon there are no more workers
                      shutdown
                      (do
                        (println "shutdown")
                        (doseq [value (vals workers)]
                           (let [{::keys [output]} value]
                             (async/>! output (assoc value
                                                     ::status false
                                                     ::event ::shutdown)))))

                      ;; must be a worker
                      (do
                        (println "worker done" value chan)
                        (recur (dissoc workers chan) shutdown)))))]
    {::control control
     ::input input
     ::output machine}))

(defn build-client
  [config]
  {::service (build-service config)
   ::executor (build-executor config)})

(defn execute-requests
  [client requests]
  (let [{::keys [executor]} client
        {::keys [input]} executor]
    (async/go
      (let [outputs (into []
                          (map (fn [request]
                                 (let [output (async/promise-chan)
                                       value {::request request
                                              ::output output}]
                                   (when-not (async/put! input value)
                                     (async/put! output {::status false
                                                         ::event ::input-closed}))
                                   output)))
                          requests)
            responses (transient [])]
        (doseq [output outputs]
          (conj! responses (async/<! output)))
        (persistent! responses)))))

(defn clear-cells-request
  [sheet-id row-count column-count]
  (-> (Request.)
      (.setUpdateSheetProperties
       (-> (UpdateSheetPropertiesRequest.)
           (.setFields "gridProperties")
           (.setProperties
            (-> (SheetProperties.)
                (.setSheetId (int sheet-id))
                (.setGridProperties
                 (-> (GridProperties.)
                     (.setRowCount (int row-count))
                     (.setColumnCount (int column-count))))))))))

(defn update-cells-request
  [sheet-id row-index column-index rows]
  (-> (Request.)
      (.setUpdateCells
       (-> (UpdateCellsRequest.)
           (.setStart
            (-> (GridCoordinate.)
                (.setSheetId (int sheet-id))
                (.setRowIndex (int row-index))
                (.setColumnIndex (int column-index))))
           (.setRows rows)
           (.setFields "userEnteredValue,userEnteredFormat")))))

(defn batch-update-request
  [^Sheets service spreadsheet-id requests]
  (-> service
      (.spreadsheets)
      (.batchUpdate
       spreadsheet-id
       (-> (BatchUpdateSpreadsheetRequest.)
           (.setRequests requests)))))

(def default-write-sheet-config
  {::cells-per-request 10000
   ::requests-per-batch 10})

(defn write-sheet!
  [client config spreadsheet-id sheet-id rows]
  (if-not (seq rows)
    (async/go ::noop)
    (let [{::keys [service]} client
          config (merge default-write-sheet-config config)
          {::keys [cells-per-request requests-per-batch]} config
          num-cols (apply max (map count rows))
          rows-per-request (long (/ cells-per-request num-cols))
          ;; We first want to clear the cells to which we're about to write
          ;; not sure why we write the first row specially but uh we do
          init-requests [(clear-cells-request sheet-id (count rows) num-cols)
                         (update-cells-request sheet-id 0 0 [(first rows)])]
          init-batch-requests [(batch-update-request service spreadsheet-id init-requests)]
          data-requests (into []
                              (map-indexed (fn [i batch]
                                             (let [row-index (inc (* i rows-per-request))]
                                               (update-cells-request sheet-id row-index 0 batch))))
                              (partition-all rows-per-request (rest rows)))
          batch-requests (into []
                               (map (fn [batch]
                                      (batch-update-request service spreadsheet-id batch)))
                               (partition-all requests-per-batch data-requests))]
      (async/go
        (let [responses (async/<! (execute-requests client init-batch-requests))]
          (if (every? true? (map ::status responses))
            (let [responses (async/<! (execute-requests client batch-requests))]
              (every? true? (map ::status responses)))
            false))))))
