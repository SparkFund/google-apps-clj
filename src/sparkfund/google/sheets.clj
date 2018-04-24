(ns sparkfund.google.sheets
  (:require [clojure.core.async :as async]
            [google-apps-clj.credentials :as credentials])
  (:import [com.google.api.client.googleapis.services
            AbstractGoogleClientRequest]
           [com.google.api.services.sheets.v4
            Sheets
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

;; TODO exponential backoff for request failures could be built into the executor
;; or it could be handled by the underlying Java library
;; via com.google.api.client.util.ExponentialBackOff
;; The former allows us more control and could allow us to continue to use
;; worker threads while in the backoff period.
;; The latter makes it Someone Else's Problem and implicitly throttles requests
;; the the service while it's sad.

(defn build-service
  [config]
  (let [{::keys [app credentials]} config]
    (.build (doto (Sheets$Builder. credentials/http-transport
                                   credentials/json-factory
                                   credentials)
              (.setApplicationName app)))))

(defn build-executor
  "Builds an executor client that execute google client requests concurrently.

   This returns an input channel and a control channel. The input channel
   expects maps of ::request ::output, where the output is a channel to which

   When the control channel closes, the client closes the input channel. All
   pending work is allowed an opportunity to finish, after which their output
   channels are closed. The executor is guaranteed to be finished cleaning up
   when the client channel closes."
  [config]
  (let [{::keys [concurrency ^Duration shutdown-duration]} config
        control (async/chan)
        input (async/chan)
        client (async/go-loop [workers {}
                               shutdown nil]
                 (let [chans (cond-> (into [] (keys workers))
                               shutdown
                               (conj shutdown)
                               (and (not shutdown (< (count workers) concurrency)))
                               input)
                       [value chan] (async/alts! chans)]
                   (condp = chan
                     input
                     (let [{::keys [^AbstractGoogleClientRequest request output]} value
                           worker (async/thread
                                    (async/put! output
                                                (try
                                                  (let [response (.execute request)]
                                                    (assoc value
                                                           ::status true
                                                           ::response response))
                                                  (catch Exception e
                                                    (assoc value
                                                           ::status false
                                                           ::exception e
                                                           ::event :execute-exception)))))]
                       (recur (assoc workers worker output) shutdown))

                     control
                     (do
                       (async/close! input)
                       (recur workers (async/timeout (.toMillis shutdown-duration))))

                     shutdown
                     (doseq [output (vals workers)]
                       (async/>! output {::status false
                                         ::event ::shutdown}))

                     ;; must be a worker
                     (recur (disj workers chan) shutdown))))]
    {::client client
     ::control control
     ::input input}))

(defn execute-requests
  [executor requests]
  (async/go
    (let [outputs (into []
                        (map (fn [request]
                               (let [output (async/promise-chan)
                                     value {::request request
                                            ::output output}]
                                 (when (async/>! executor value)
                                   output))))
                        requests)]
      (into []
            (map (fn [output]
                   (when output
                     (async/<! output))))
            outputs))))

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
  [config executor service spreadsheet-id sheet-id rows]
  (when (seq rows)
    (let [config (merge default-write-sheet-config config)
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
        (let [responses (async/<! (execute-requests executor init-batch-requests))]
          (when (every? true? responses)
            (every? true? (async/<! (execute-requests executor batch-requests)))))))))
