(ns google-apps-clj.google-sheets-v4
  (:require [clj-time.core :as time])
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

(defn build-service
  [client-id client-secret]
  (let [http-transport (GoogleNetHttpTransport/newTrustedTransport)
        scopes [SheetsScopes/SPREADSHEETS]
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
    service))

(defn get-sheet-info
  "Returns a \"sheets\" field which contains information about a spreadsheet's
  sheets (tabs). Includes \"sheetId\" which is needed for batch updates."
  [service spreadsheet-id]
  (-> service
      (.spreadsheets)
      (.get spreadsheet-id)
      (.execute)))

(defn coerce-to-cell-data
  "Numbers and strings and keywords auto-coerce to CellData"
  [val]
  (cond
    (number? val) (-> (CellData.)
                      (.setUserEnteredValue
                       (-> (ExtendedValue.)
                           (.setNumberValue (double val)))))
    (string? val) (-> (CellData.)
                      (.setUserEnteredValue
                       (-> (ExtendedValue.)
                           (.setStringValue val))))
    (keyword? val) (-> (CellData.)
                       (.setUserEnteredValue
                        (-> (ExtendedValue.)
                            (.setStringValue (str val)))))
    (instance? CellData val) val
    :else (throw (ex-info
                  (str "Unknown cell type: " (type val))
                  {:val val
                   :type (type val)}))))

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

(defn date-time
  "using the given excel date formatting string"
  [date-time format-patterm]
  (-> (CellData.)
      (.setUserEnteredValue
       (-> (ExtendedValue.)
           ;; Heaven help us. Excel serial time and casting to a float.
           (.setNumberValue
            (double (+ 2 (time/in-days (time/interval (time/date-time 1900 1 1)
                                                      date-time)))))))
      (.setUserEnteredFormat
       (-> (CellFormat.)
           (.setNumberFormat
            (-> (NumberFormat.)
                (.setType "DATE")
                (.setPattern format-patterm)))))))

(defn row->row-data
  "google-ifies a row (list of columns) of type string?, number? keyword? or CellData."
  [row]
  (-> (RowData.)
      (.setValues (map coerce-to-cell-data row))))

(defn write-sheet
  "writes values to a specific sheet (tab). Breaks down requests into batches of ~10k cells."
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
                         (.setDeleteDimension (-> (DeleteDimensionRequest.)
                                                  (.setRange (-> (DimensionRange.)
                                                                 (.setDimension "COLUMNS")
                                                                 (.setSheetId sheet-id)
                                                                 (.setStartIndex (int 1))
                                                                 (.setEndIndex (int 9999)))))))
                     (-> (Request.)
                         (.setInsertDimension (-> (InsertDimensionRequest.)
                                                  (.setInheritFromBefore true)
                                                  (.setRange (-> (DimensionRange.)
                                                                 (.setDimension "COLUMNS")
                                                                 (.setSheetId sheet-id)
                                                                 (.setStartIndex (int 1))
                                                                 (.setEndIndex num-cols))))))
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
  [service worksheet-id sheet-title]
  (let [info (get-sheet-info service worksheet-id)
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
  [service worksheet-id sheet-title table & {:keys [force?]}]
  (let [info (get-sheet-info service worksheet-id)
        add-sheet-id (fn [] (-> (add-sheet service worksheet-id sheet-title)
                                (get "sheetId")))
        sheet-id (if force?
                   (or (find-sheet-by-title service worksheet-id sheet-title)
                       (add-sheet-id))
                   (add-sheet-id))]
    (write-sheet service worksheet-id sheet-id table)
    sheet-id))
