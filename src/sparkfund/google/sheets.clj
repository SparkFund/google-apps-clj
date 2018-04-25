(ns sparkfund.google.sheets
  (:require [clj-time.core :as time]
            [clojure.core.async :as async]
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
            CellData
            CellFormat
            ExtendedValue
            NumberFormat
            GridCoordinate
            GridProperties
            Request
            RowData
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
  (let [{::keys [^String app]} config
        request-initializer (build-request-initializer config)]
    (.build (doto (Sheets$Builder. credentials/http-transport
                                   credentials/json-factory
                                   request-initializer)
              (.setApplicationName app)))))

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
    {::service (build-service config)
     ::input input
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

(defprotocol ToCellData
  (datum->cell ^CellData [_]))

(defn joda-date-cell
  "Returns a CellData containing the given date, formatted using the given excel
   date formatting string, e.g. \"yyyy-mm-dd\" or \"M/d/yyyy\""
  [pattern dt]
  (-> (CellData.)
      (.setUserEnteredValue
       (-> (ExtendedValue.)
           ;; https://developers.google.com/sheets/api/guides/concepts#datetime_serial_numbers
           (.setNumberValue
            (double (time/in-days (time/interval (time/date-time 1899 12 30) dt))))))
      (.setUserEnteredFormat
       (-> (CellFormat.)
           (.setNumberFormat
            (-> (NumberFormat.)
                (.setType "DATE")
                (.setPattern pattern)))))))

(defn safe-to-double?
  [n]
  (= (bigdec n) (bigdec (double n))))

(extend-protocol ToCellData
  Number
  (datum->cell [n]
    (when-not (safe-to-double? n)
      (throw (ex-info "Number value exceeds double precision" {:n n})))
    (-> (CellData.)
        (.setUserEnteredValue
         (-> (ExtendedValue.)
             (.setNumberValue (double n))))))
  String
  (datum->cell [s]
    (-> (CellData.)
        (.setUserEnteredValue
         (-> (ExtendedValue.)
             (.setStringValue s)))))
  clojure.lang.Keyword
  (datum->cell [kw]
    (datum->cell (str kw)))
  CellData
  (datum->cell [cd]
    cd)
  org.joda.time.DateTime
  (datum->cell [dt]
    (joda-date-cell "yyyy-mm-dd" dt))
  nil
  (datum->cell [_]
    (CellData.)))

(defn data->row
  [row]
  (-> (RowData.)
      (.setValues (map datum->cell row))))

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
           (.setRows (map data->row rows))
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
  (let [num-cols (apply max (map count rows))]
    (if-not (pos? num-cols)
      (async/go ::noop)
      (let [{::keys [service]} client
            config (merge default-write-sheet-config config)
            {::keys [cells-per-request requests-per-batch]} config
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
            (if (every? true? (map ::success? responses))
              (let [responses (async/<! (execute-requests client batch-requests))]
                (every? true? (map ::success? responses)))
              false)))))))
