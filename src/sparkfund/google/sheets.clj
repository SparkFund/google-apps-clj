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
  [client service config spreadsheet-id sheet-id rows]
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
          (let [responses (async/<! (client/execute-requests client init-batch-requests))]
            (if (every? true? (map ::success? responses))
              (let [responses (async/<! (client/execute-requests client batch-requests))]
                (every? true? (map ::success? responses)))
              false)))))))
