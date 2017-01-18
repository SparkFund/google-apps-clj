(ns google-apps-clj.google-sheets-v4
  (:require [clj-time.core :as time]
            [clojure.string :as string]
            [google-apps-clj.credentials :as cred])
  (:import
   (com.google.api.client.auth.oauth2 Credential)
   (com.google.api.client.extensions.java6.auth.oauth2 AuthorizationCodeInstalledApp)
   (com.google.api.client.extensions.jetty.auth.oauth2 LocalServerReceiver)
   (com.google.api.client.googleapis.auth.oauth2 GoogleAuthorizationCodeFlow
                                                 GoogleAuthorizationCodeFlow$Builder
                                                 GoogleClientSecrets)
   (com.google.api.client.googleapis.javanet GoogleNetHttpTransport)
   (com.google.api.client.json.jackson2 JacksonFactory)
   (com.google.api.services.sheets.v4 SheetsScopes
                                      Sheets
                                      Sheets$Builder)
   (com.google.api.services.sheets.v4.model AddSheetRequest
                                            AppendCellsRequest
                                            BatchUpdateSpreadsheetRequest
                                            CellData
                                            CellFormat
                                            DeleteDimensionRequest
                                            DimensionRange
                                            ExtendedValue
                                            GridCoordinate
                                            GridProperties
                                            InsertDimensionRequest
                                            NumberFormat
                                            Request
                                            RowData
                                            SheetProperties
                                            UpdateCellsRequest
                                            UpdateSheetPropertiesRequest)))

(def scopes
  [SheetsScopes/SPREADSHEETS])

(defn ^Sheets build-service
  ([google-ctx]
   (let [creds (cred/build-credential google-ctx)
         builder (Sheets$Builder. cred/http-transport cred/json-factory creds)]
     (doto builder
       (.setApplicationName "google-apps-clj"))
     (.build builder)))
  ([client-id client-secret]
   (let [http-transport (GoogleNetHttpTransport/newTrustedTransport)
         auth-flow (-> (GoogleAuthorizationCodeFlow$Builder. http-transport
                                                             (JacksonFactory/getDefaultInstance)
                                                             client-id
                                                             client-secret
                                                             scopes)
                       (.setAccessType "offline")
                       (.build))
         credential (-> (AuthorizationCodeInstalledApp. auth-flow (LocalServerReceiver.))
                        (.authorize nil))
         service (-> (Sheets$Builder. http-transport (JacksonFactory/getDefaultInstance) credential)
                     (.setApplicationName "Application test")
                     (.build))]
     service)))

(defn get-spreadsheet-info
  "Returns a \"sheets\" field which contains information about a spreadsheet's
  sheets (tabs). Includes \"sheetId\" which is needed for batch updates."
  [^Sheets service spreadsheet-id]
  (-> service
      (.spreadsheets)
      (.get spreadsheet-id)
      (.execute)))

(defn get-sheet-info
  [service spreadsheet-id sheet-id]
  (let [sheets (get (get-spreadsheet-info service spreadsheet-id) "sheets")]
    (some (fn [sheet]
            (let [properties (get sheet "properties")]
              (when (= sheet-id (get properties "sheetId"))
                properties)))
          sheets)))

(defn get-sheet-titles
  [service spreadsheet-id]
  "returns a list of [sheet-title sheet-id] tuples. It seems the order reflects
  the order of the tabs in google's interface, though I doubt this is anywhere
  guaranteed."
  (->> (get (get-spreadsheet-info service spreadsheet-id) "sheets")
       (map #(get % "properties"))
       (mapv (juxt #(get % "title") #(get % "sheetId")))))

(defn date-cell
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

(defn formula-cell
  "Returns a CellData containing the given formula"
  [str]
  (-> (CellData.)
      (.setUserEnteredValue
       (-> (ExtendedValue.)
           (.setFormulaValue str)))))

(defn safe-to-double?
  [n]
  (= (bigdec n) (bigdec (double n))))

(defprotocol CellDataValue
  (->cell-data ^CellData [_]))

(extend-protocol CellDataValue
  Number
  (->cell-data [n]
    (when-not (safe-to-double? n)
      (throw (ex-info "Number value exceeds double precision" {:n n})))
    (-> (CellData.)
        (.setUserEnteredValue
         (-> (ExtendedValue.)
             (.setNumberValue (double n))))))
  String
  (->cell-data [s]
    (-> (CellData.)
        (.setUserEnteredValue
         (-> (ExtendedValue.)
             (.setStringValue s)))))
  clojure.lang.Keyword
  (->cell-data [kw]
    (->cell-data (str kw)))
  CellData
  (->cell-data [cd]
    cd)
  org.joda.time.DateTime
  (->cell-data [dt]
    (date-cell "yyyy-mm-dd" dt))
  nil
  (->cell-data [_]
    (CellData.)))

(defn coerce-to-cell
  "Numbers and strings and keywords and date-times auto-coerce to CellData"
  [x]
  (->cell-data x))

(defn currency-cell
  "Returns a CellData containing the given value formatted as currency"
  [v]
  (-> (->cell-data v)
      (.setUserEnteredFormat
       (-> (CellFormat.)
           (.setNumberFormat
            (-> (NumberFormat.)
                (.setType "CURRENCY")))))))

(defn cell->clj
  "Converts cell data with either a userEnteredValue (x)or effectiveValue to a clojure type.
  stringValue -> string
  numberValue -> double
  DATE -> date-time
  else ~ identity"
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

(defn row->row-data
  "google-ifies a row (list of columns) of type string?, number? keyword? or CellData."
  [row]
  (-> (RowData.)
      (.setValues (map coerce-to-cell row))))

(def default-write-sheet-options
  {:batch-size 10000})

(defn write-sheet
  "Overwrites the given sheet with the given rows of data. The data on the given
   sheet will be deleted and it will be resized to fit the given data exactly.

   This will be batched into requests of approximately 10k cell values. Larger
   requests yielded errors, though there is apparently no explicit limit or
   guidance given."
  ([service spreadsheet-id sheet-id rows]
   (write-sheet service spreadsheet-id sheet-id rows {}))
  ([^Sheets service spreadsheet-id sheet-id rows options]
   (assert (not-empty rows) "Must write at least one row to the sheet")
   (let [{:keys [batch-size]} (merge default-write-sheet-options options)
         sheet-id (int sheet-id)
         num-cols (int (apply max (map count rows)))
         first-row (first rows)
         part-size (long (/ batch-size num-cols))
         rest-batches (partition-all part-size (rest rows))
         first-batch (concat
                      [(-> (Request.)
                          (.setUpdateSheetProperties
                           (-> (UpdateSheetPropertiesRequest.)
                               (.setFields "gridProperties")
                               (.setProperties
                                (-> (SheetProperties.)
                                    (.setSheetId sheet-id)
                                    (.setGridProperties
                                     (-> (GridProperties.)
                                         (.setRowCount (int (count rows)))
                                          (.setColumnCount (int num-cols)))))))))]
                      (when (< 0 (count rows))
                        [(-> (Request.)
                          (.setUpdateCells
                           (-> (UpdateCellsRequest.)
                               (.setStart
                                (-> (GridCoordinate.)
                                    (.setSheetId sheet-id)
                                    (.setRowIndex (int 0))
                                    (.setColumnIndex (int 0))))
                               (.setRows [(row->row-data first-row)])
                                  (.setFields "userEnteredValue,userEnteredFormat"))))])
                      (when (< 1 (count rows))
                        [(-> (Request.)
                          (.setUpdateCells
                           (-> (UpdateCellsRequest.)
                               (.setStart
                                (-> (GridCoordinate.)
                                    (.setSheetId sheet-id)
                                    (.setRowIndex (int 1))
                                    (.setColumnIndex (int 0))))
                               (.setRows (map row->row-data (first rest-batches)))
                                  (.setFields "userEnteredValue,userEnteredFormat"))))]))]
     (-> service
         (.spreadsheets)
         (.batchUpdate
          spreadsheet-id
          (-> (BatchUpdateSpreadsheetRequest.)
              (.setRequests first-batch)))
         (.execute))
     (loop [row-index (inc (count (first rest-batches)))
            batches (rest rest-batches)]
       (when (seq batches)
         (-> service
             (.spreadsheets)
             (.batchUpdate
              spreadsheet-id
              (-> (BatchUpdateSpreadsheetRequest.)
                  (.setRequests
                   [(-> (Request.)
                        (.setUpdateCells
                         (-> (UpdateCellsRequest.)
                             (.setStart
                              (-> (GridCoordinate.)
                                  (.setSheetId sheet-id)
                                  (.setRowIndex (int row-index))
                                  (.setColumnIndex (int 0))))
                             (.setRows (map row->row-data (first batches)))
                             (.setFields "userEnteredValue,userEnteredFormat"))))])))
             (.execute))
         (recur (+ row-index (count (first batches)))
                (rest batches)))))))

(defn append-sheet
  "appends rows to a specific sheet (tab). Appends starting at the last
  non-blank row.  Breaks down requests into batches of ~10k cells.  Doesn't
  alter the number of columns on the sheet and so writing more columns than the
  sheet has will error"
  [^Sheets service spreadsheet-id sheet-id rows]
  (assert (not-empty rows) "Must write at least one row to the sheet")
  (let [sheet-id (int sheet-id)
        num-cols (int (count (first rows)))
        part-size (long (/ 10000 num-cols))
        batches (partition part-size part-size [] rows)
        first-batch [(-> (Request.)
                         (.setAppendCells (-> (AppendCellsRequest.)
                                              (.setSheetId sheet-id)
                                              (.setRows (map row->row-data (first batches)))
                                              (.setFields "userEnteredValue,userEnteredFormat"))))]]
    (doall (cons (-> service
                     (.spreadsheets)
                     (.batchUpdate spreadsheet-id
                                   (-> (BatchUpdateSpreadsheetRequest.) (.setRequests first-batch)))
                     (.execute))
                 (map (fn [batch]
                        (-> service
                            (.spreadsheets)
                            (.batchUpdate
                             spreadsheet-id
                             (-> (BatchUpdateSpreadsheetRequest.)
                                 (.setRequests [(-> (Request.)
                                                    (.setAppendCells
                                                     (-> (AppendCellsRequest.)
                                                         (.setSheetId sheet-id)
                                                         (.setRows (map row->row-data batch))
                                                         (.setFields "userEnteredValue,userEnteredFormat"))))])))
                            (.execute)))
                      (rest batches))))))

(defn add-sheet
  "returns the 'properties' field of the created sheet"
  [^Sheets service spreadsheet-id sheet-title]
  (let [response (-> service
                     (.spreadsheets)
                     (.batchUpdate
                      spreadsheet-id
                      (-> (BatchUpdateSpreadsheetRequest.)
                          (.setRequests
                           [(-> (Request.)
                                (.setAddSheet
                                 (-> (AddSheetRequest.)
                                     (.setProperties
                                      (-> (SheetProperties.)
                                          (.setIndex (int 0))
                                          (.setTitle sheet-title))))))])))
                     (.execute))]
    (-> response
        (get "replies")
        (first)
        (get-in ["addSheet" "properties"]))))

(defn find-sheet-id
  "Returns the id of the sheet with the given title in the given spreadsheet
   id, if any. If there are more than one sheets with the given title, this
   raises an exception."
  [service spreadsheet-id sheet-title]
  (let [info (get-spreadsheet-info service spreadsheet-id)
        sheet-ids (->> (get-in info ["sheets"])
                       (filter #(= sheet-title (get-in % ["properties" "title"])))
                       (map #(get-in % ["properties" "sheetId"])))]
    (case (count sheet-ids)
      1 (first sheet-ids)
      0 nil)))

(defn obtain-sheet-id
  "Returns the id of a sheet with the given title in the given spreadsheet id.
   If one already exists, this returns it, otherwise creates a new one."
  [service spreadsheet-id sheet-title]
  (or (find-sheet-id service spreadsheet-id sheet-title)
      (get (add-sheet service spreadsheet-id sheet-title) "sheetId")))

(defn get-cells
  "sheet-ranges is a seq of strings, using the A1 syntax, eg [\"Sheet!A1:Z9\"]
   Returns a vector of tables in corresponding to sheet-ranges.  Only one
   sheet (tab) can be specified per batch, due to a quirk of Google's API as far
   as we can tell."
  [^Sheets service spreadsheet-id sheet-ranges]
  (let [sheet-titles (map #(-> % (string/split #"!") first) sheet-ranges)
        _ (when (not= sheet-titles (distinct sheet-titles))
            (throw (ex-info "Can't query the same sheet twice in the same batch"
                            {:sheet-ranges sheet-ranges})))
        fields "sheets(properties(title),data(rowData(values(effectiveValue,userEnteredFormat))))"
        data (-> service
                 (.spreadsheets)
                 (.get spreadsheet-id)
                 (.setRanges sheet-ranges)
                 (.setFields fields)
                 (.execute))
        tables (get data "sheets")
        title->table (->> tables
                          (map (fn [table]
                                 (let [title (get-in table ["properties" "title"])
                                       rows (mapv #(get % "values")
                                                  (-> (get table "data")
                                                      first
                                                      (get "rowData")))]
                                   [title rows])))
                          (into {}))]
    (mapv title->table sheet-titles)))

(defn get-cell-values
  "sheet-ranges is a seq of strings, using the A1 syntax, eg [\"Sheet!A1:Z9\"]
   Returns a vector of tables in corresponding to sheet-ranges.  Only one
   sheet (tab) can be specified per batch, due to a quirk of Google's API as far
   as we can tell."
  [^Sheets service spreadsheet-id sheet-ranges]
  (let [tables (get-cells service spreadsheet-id sheet-ranges)]
    (mapv (partial mapv (partial mapv cell->clj)) tables)))
