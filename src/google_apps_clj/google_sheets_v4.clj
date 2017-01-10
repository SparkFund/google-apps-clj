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
   (com.google.api.client.http HttpTransport)
   (com.google.api.client.json.jackson2 JacksonFactory)
   (com.google.api.client.json JsonFactory)
   (com.google.api.client.util.store FileDataStoreFactory)
   (com.google.api.services.sheets.v4 SheetsScopes
                                      Sheets
                                      Sheets$Builder
                                      SheetsRequestInitializer)
   (com.google.api.services.sheets.v4.model AddSheetRequest
                                            AppendCellsRequest
                                            BatchUpdateSpreadsheetRequest
                                            CellData
                                            CellFormat
                                            DeleteDimensionRequest
                                            DimensionRange
                                            ExtendedValue
                                            GridCoordinate
                                            InsertDimensionRequest
                                            NumberFormat
                                            Request
                                            RowData
                                            SheetProperties
                                            UpdateCellsRequest)
   (java.io IOException
            InputStream
            InputStreamReader)
   (java.util Arrays
              List)))

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
  [service spreadsheet-id]
  (-> service
      (.spreadsheets)
      (.get spreadsheet-id)
      (.execute)))

(defn get-sheet-titles
  [service spreadsheet-id]
  "returns a list of [sheet-title sheet-id] tuples. It seems the order reflects
  the order of the tabs in google's interface, though I doubt this is anywhere
  guaranteed."
  (->> (get (get-spreadsheet-info service spreadsheet-id) "sheets")
       (map #(get % "properties"))
       (mapv (juxt #(get % "title") #(get % "sheetId")))))

(defn date-time
  "using the given excel date formatting string, eg \"yyyy-mm-dd\", \"M/d/yyyy\" "
  [date-time format-patterm]
  (-> (CellData.)
      (.setUserEnteredValue
       (-> (ExtendedValue.)
           ;; https://developers.google.com/sheets/api/guides/concepts#datetime_serial_numbers
           (.setNumberValue
            (double (time/in-days (time/interval (time/date-time 1899 12 30)
                                                 date-time))))))
      (.setUserEnteredFormat
       (-> (CellFormat.)
           (.setNumberFormat
            (-> (NumberFormat.)
                (.setType "DATE")
                (.setPattern format-patterm)))))))

(defn safe-to-double?
  [n]
  (= (bigdec n) (bigdec (double n))))

(defprotocol CellDataValue
  (->cell-data [_]))

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
    (date-time dt "yyyy-mm-dd"))
  nil
  (->cell-data [_]
    (CellData.)))

(defn coerce-to-cell-data
  "Numbers and strings and keywords and date-times auto-coerce to CellData"
  [x]
  (->cell-data x))

(defn cell-data->clj
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
        date? (= "DATE" number-format)
        currency? (= "CURRENCY" number-format)
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

(defn formula
  [str]
  (-> (CellData.)
      (.setUserEnteredValue
       (-> (ExtendedValue.)
           (.setFormulaValue str)))))

(defn with-money-format
  "Adds money formatting to CellData (or other val corecible by coerce-to-cell-data)"
  [coercible-val]
  (let [cell-data (coerce-to-cell-data coercible-val)]
    (-> cell-data
        (.setUserEnteredFormat
         (-> (CellFormat.)
             (.setNumberFormat
              (-> (NumberFormat.)
                  (.setType "CURRENCY"))))))))

(defn row->row-data
  "google-ifies a row (list of columns) of type string?, number? keyword? or CellData."
  [row]
  (-> (RowData.)
      (.setValues (map coerce-to-cell-data row))))

(defn write-sheet
  "writes values to a specific sheet (tab). Breaks down requests into batches of ~10k cells.
  Overwrites all the existing rows on the sheet.  Doesn't alter the number of
  columns on the sheet and so writing more columns than the sheet has will error"
  [service spreadsheet-id sheet-id rows]
  (assert (not-empty rows) "Must write at least one row to the sheet")
  (let [sheet-id (int sheet-id)
        num-cols (int (count (first rows)))
        first-row (first rows)
        part-size (long (/ 10000 num-cols))
        rest-batches (partition part-size part-size [] (rest rows))
        first-batch [(-> (Request.)
                         (.setDeleteDimension (-> (DeleteDimensionRequest.)
                                                  (.setRange (-> (DimensionRange.)
                                                                 (.setDimension "ROWS")
                                                                 (.setSheetId sheet-id)
                                                                 (.setStartIndex (int 1))
                                                                 (.setEndIndex (int 9999)))))))
                     (-> (Request.)
                         (.setUpdateCells (-> (UpdateCellsRequest.)
                                              (.setStart (-> (GridCoordinate.)
                                                             (.setSheetId sheet-id)
                                                             (.setRowIndex (int 0))
                                                             (.setColumnIndex (int 0))))
                                              (.setRows [(row->row-data first-row)])
                                              (.setFields "userEnteredValue,userEnteredFormat"))))
                     (-> (Request.)
                         (.setAppendCells (-> (AppendCellsRequest.)
                                              (.setSheetId sheet-id)
                                              (.setRows (map row->row-data (first rest-batches)))
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
                      (rest rest-batches))))))

(defn append-sheet
  "appends rows to a specific sheet (tab). Appends starting at the last
  non-blank row.  Breaks down requests into batches of ~10k cells.  Doesn't
  alter the number of columns on the sheet and so writing more columns than the
  sheet has will error"
  [service spreadsheet-id sheet-id rows]
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
  [service spreadsheet-id sheet-title]
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

(defn find-sheet-by-title
  "returns the sheetId or nil"
  [service spreadsheet-id sheet-title]
  (let [info (get-spreadsheet-info service spreadsheet-id)
        sheet-ids (->> (get-in info ["sheets"])
                       (filter #(= sheet-title (get-in % ["properties" "title"])))
                       (map #(get-in % ["properties" "sheetId"])))]
    (case (count sheet-ids)
      1 (first sheet-ids)
      0 nil)))

(defn add-sheet-with-data
  "Adds a new sheet (tab) with the given table data,
   Will throw an exception if the sheet already exists, unless :force? is true
   Returns the sheet id."
  [service spreadsheet-id sheet-title table & {:keys [force?]}]
  (let [info (get-spreadsheet-info service spreadsheet-id)
        add-sheet-id (fn [] (-> (add-sheet service spreadsheet-id sheet-title)
                                (get "sheetId")))
        sheet-id (if force?
                   (or (find-sheet-by-title service spreadsheet-id sheet-title)
                       (add-sheet-id))
                   (add-sheet-id))]
    (write-sheet service spreadsheet-id sheet-id table)
    spreadsheet-id))

(defn get-effective-vals
  "sheet-ranges is a seq of strings, using the A1 syntax, eg [\"Sheet!A1:Z9\"]
   Returns a vector of tables in corresponding to sheet-ranges.  Only one
   sheet (tab) can be specified per batch, due to a quirk of Google's API as far
   as we can tell."
  [service spreadsheet-id sheet-ranges]
  (let [sheet-titles (map #(-> % (string/split #"!") first) sheet-ranges)
        _ (when (not= sheet-titles (distinct sheet-titles))
            (throw (ex-info "Can't query the same sheet twice in the same batch" {:sheet-ranges sheet-ranges})))
        data (-> service
                 (.spreadsheets)
                 (.get spreadsheet-id)
                 (.setRanges sheet-ranges)
                 (.setFields "sheets(properties(title),data(rowData(values(effectiveValue,userEnteredFormat))))")
                 (.execute))
        tables (-> data
                   (get "sheets"))
        title->table (->> tables
                          (map (fn [table]
                                 (let [rows (-> table (get "data") (first) (get "rowData"))
                                       cljd-rows (->> rows
                                                      (mapv (fn [row]
                                                              (let [vals (-> row (get "values"))]
                                                                (->> vals
                                                                     (mapv (fn [val] (-> val
                                                                                         (cell-data->clj)))))))))]
                                   [(get-in table ["properties" "title"]) cljd-rows])))
                          (into {}))]
    (mapv title->table sheet-titles)))
