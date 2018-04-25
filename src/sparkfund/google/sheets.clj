(ns sparkfund.google.sheets
  (:require [clj-time.core :as time]
            [clojure.core.async :as async]
            [sparkfund.google.client :as client])
  (:import
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

(def scopes
  [SheetsScopes/SPREADSHEETS])

(defn build-service
  [config]
  (let [{::keys [^String app]} config
        scope SheetsScopes/SPREADSHEETS
        config (update config ::client/scopes (fnil conj #{}) scope)
        request-initializer (client/build-request-initializer config)]
    (.build (doto (Sheets$Builder. client/http-transport
                                   client/json-factory
                                   request-initializer)
              (.setApplicationName app)))))

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

(defn cell-to-datum
  [cell-data]
  (let [ev (get cell-data "effectiveValue")
        uev (get cell-data "userEnteredValue")
        v (or ev uev)
        string-val (get v "stringValue")
        number-val (get v "numberValue")
        number-format (get-in cell-data ["userEnteredFormat" "numberFormat" "type"])
        date? (and (= "DATE" number-format) (some? number-val))
        currency? (and (= "CURRENCY" number-format) (some? number-val))
        empty-cell? (and (nil? ev) (nil? uev) (instance? CellData cell-data))]
    (when (and (some? ev)
               (some? uev))
      (throw (ex-info "Ambiguous cell data, contains both string effectiveValue and userEnteredValue"
                      {:cell-data cell-data})))
    (when (and (some? string-val)
               (some? number-val))
      (throw (ex-info "Ambiguous cell data value, contains both stringValue and numberValue"
                      {:cell-data cell-data})))
    (cond
      string-val
      string-val

      ;; TODO how might we control the return type if we don't want joda time?
      date?
      ;; https://developers.google.com/sheets/api/guides/concepts#datetime_serial_numbers
      (time/plus (time/date-time 1899 12 30) (time/days (long number-val)))

      currency?
      (bigdec number-val)

      number-val
      number-val

      empty-cell?
      nil

      :else
      cell-data)))

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

(defn get-cells-request
  [^Sheets service spreadsheet-id sheet-ranges]
  (let [fields "sheets(properties(title),data(rowData(values(effectiveValue,userEnteredFormat))))"]
    (-> service
        (.spreadsheets)
        (.get spreadsheet-id)
        (.setRanges sheet-ranges)
        (.setFields fields))))

(def default-write-sheet-config
  {::cells-per-request 10000
   ::requests-per-batch 10})

(defn write-sheet!
  [client service config spreadsheet-id sheet-id rows]
  (let [num-cols (apply max (map count rows))]
    (if-not (pos? num-cols)
      (async/go ::noop)
      (let [config (merge default-write-sheet-config config)
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
          (let [responses (async/<! (client/execute-requests client init-batch-requests))]
            (if (every? true? (map ::client/success? responses))
              (let [responses (async/<! (client/execute-requests client batch-requests))]
                (every? true? (map ::client/success? responses)))
              false)))))))

(defn read-sheet!
  [client service spreadsheet-id sheet-title]
  (let [request (get-cells-request service spreadsheet-id [sheet-title])]
    (async/go
      (let [[response] (async/<! (client/execute-requests client [request]))
            {::client/keys [response success?]} response]
        (when success?
          (into []
                (comp (map (fn [row-data]
                             (get row-data "values")))
                      (map (fn [row]
                             (into [] (map cell-to-datum) row))))
                (-> response
                    (get "sheets")
                    first
                    (get "data")
                    first
                    (get "rowData"))))))))
