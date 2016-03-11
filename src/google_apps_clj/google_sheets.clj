(ns google-apps-clj.google-sheets
  (:require [clojure.core.typed :as t]
            [clojure.core.typed.unsafe :as tu]
            [clojure.edn :as edn :only [read-string]]
            [clojure.java.io :as io :only [as-url file resource]]
            [google-apps-clj.credentials :as cred])
  (:import (com.google.gdata.data.spreadsheet CellEntry
                                              CellFeed
                                              ListEntry
                                              ListFeed
                                              SpreadsheetEntry
                                              SpreadsheetFeed
                                              WorksheetEntry
                                              WorksheetFeed
                                              CustomElementCollection)
           (com.google.gdata.data ILink$Rel
                                  ILink$Type
                                  PlainTextConstruct)
           (com.google.gdata.client.spreadsheet CellQuery
                                                SpreadsheetQuery
                                                SpreadsheetService
                                                WorksheetQuery)
           (com.google.gdata.data.batch BatchOperationType
                                        BatchUtils)
           (java.net URL)))

(t/ann ^:no-check clojure.java.io/as-url [t/Str -> URL])

(def spreadsheet-url
  "The url needed and used to recieve a spreadsheet feed"
  (io/as-url "https://spreadsheets.google.com/feeds/spreadsheets/private/full"))

(t/ann build-sheet-service [cred/GoogleAuth -> SpreadsheetService])
(defn build-sheet-service
  "Given a google-ctx configuration map, builds a SpreadsheetService using
   the credentials coming from google-ctx"
  [google-ctx]
  ;; TODO hilariously, drive and sheets java objects specify timeouts
  ;; by apparently completely incompatible mechanisms. Here, we just
  ;; explicitly ignore any timeouts while I pause and reflect on my
  ;; increasingly bad life choices
  (let [google-ctx (if (map? google-ctx)
                     (dissoc google-ctx :read-timeout :open-timeout)
                     google-ctx)
        cred (cred/build-credential google-ctx)
        service (SpreadsheetService. "Default Spreadsheet Service")]
    (doto service
      (.setOAuth2Credentials cred))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;; Spreadsheet Entry Functions ;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(t/ann find-spreadsheet-by-id
       [SpreadsheetService String -> (t/U '{:spreadsheet SpreadsheetEntry}
                                          '{:error (t/Val :no-entry)})])
(defn find-spreadsheet-by-id
  "Given a SpreadsheetService and the id of a spreadsheet, find the SpreadsheetEntry
   with the given id in a map, or an error message in a map"
  [^SpreadsheetService sheet-service id]
  (let [sheet-url (io/as-url (str spreadsheet-url "/" id))
        etag nil
        entry (tu/ignore-with-unchecked-cast
               (.getEntry sheet-service sheet-url SpreadsheetEntry ^String etag)
               SpreadsheetEntry)]
    (if entry
      {:spreadsheet entry}
      {:error :no-entry})))


(t/ann find-spreadsheet-by-title
       [SpreadsheetService String -> (t/U '{:spreadsheet SpreadsheetEntry}
                                          '{:error t/Keyword})])
(defn find-spreadsheet-by-title
  "Given a SpreadsheetService and a title of a spreadsheet, find the SpreadsheetEntry
   with the given title in a map, or an error message in a map"
  [sheet-service title]
  (let [query (doto (SpreadsheetQuery. spreadsheet-url)
                (.setTitleQuery title)
                (.setTitleExact true))
        query-results (doto (.query ^SpreadsheetService sheet-service query SpreadsheetFeed)
                        assert)
        entries (doto (.getEntries query-results)
                  assert)]
    (cond (= 1 (count entries)) {:spreadsheet (doto (cast SpreadsheetEntry (first entries))
                                                assert)}
          (< (count entries) 1) {:error :no-spreadsheet}
          :else {:error :more-than-one-spreadsheet})))

(t/ann ^:no-check file-name->ids [cred/GoogleAuth String -> (t/U '{:spreadsheet t/Map
                                                                  :worksheet t/Map}
                                                                '{:error t/Keyword})])
(defn file-name->ids
  "Given a google-ctx, and a spreadsheet name, gets the spreadsheet id and all of the
   worksheet ids for this file and outputs them as a map"
  [google-ctx spreadsheet-name]
  (let [sheet-service (build-sheet-service google-ctx)
        spreadsheet (find-spreadsheet-by-title sheet-service spreadsheet-name)]
    (if (contains? spreadsheet :error)
      spreadsheet
      (let [spreadsheet-entry ^SpreadsheetEntry (:spreadsheet spreadsheet)
            spreadsheet-id (.getId spreadsheet-entry)
            spreadsheet-id (subs spreadsheet-id (inc (.lastIndexOf spreadsheet-id "/")))
            worksheets (seq (.getWorksheets spreadsheet-entry))
            get-id (fn [^WorksheetEntry worksheet-entry]
                     (let [worksheet-id (.getId worksheet-entry)]
                       [(subs worksheet-id (inc (.lastIndexOf worksheet-id "/")))
                        (.getPlainText (.getTitle worksheet-entry))]))
            all-worksheets (map get-id worksheets)
            worksheet-map (into {} all-worksheets)]
        {:spreadsheet {spreadsheet-id spreadsheet-name}
         :worksheets worksheet-map}))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;   Worksheet Entry Functions  ;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(t/ann create-new-worksheet
       [cred/GoogleAuth SpreadsheetEntry Number Number String -> WorksheetEntry])
(defn create-new-worksheet
  "Given a google-ctx configuration map, SpreadsheetEntry, rows, columns, and
   a title, create a new worksheet for for the SpreadsheetEntry with this data"
  [google-ctx spreadsheet-entry rows cols title]
  (let [sheet-service (build-sheet-service google-ctx)
        worksheet (doto (WorksheetEntry.)
                    (.setTitle (PlainTextConstruct. title))
                    (.setRowCount (int rows))
                    (.setColCount (int cols)))
        feed-url (doto (.getWorksheetFeedUrl ^SpreadsheetEntry spreadsheet-entry)
                   assert)]
    (cast WorksheetEntry (doto (.insert ^SpreadsheetService sheet-service feed-url worksheet)
                           assert))))

(t/ann update-worksheet-row-count [WorksheetEntry Number -> WorksheetEntry])
(defn update-worksheet-row-count
  "Given a WorksheetEntry and desired amount of rows, edit and
   return the new WorkSheetEntry"
  [worksheet-entry rows]
  (let [worksheet (doto ^WorksheetEntry worksheet-entry
                        (.setRowCount (int rows)))]
    (cast WorksheetEntry (doto (.update worksheet)
                           assert))))

(t/ann update-worksheet-col-count [WorksheetEntry Number -> WorksheetEntry])
(defn update-worksheet-col-count
  "Given a WorksheetEntry and desired amount of columns, edit and
   return the new WorkSheetEntry"
  [worksheet-entry cols]
  (let [worksheet (doto ^WorksheetEntry worksheet-entry
                        (.setColCount (int cols)))]
    (cast WorksheetEntry (doto (.update worksheet)
                           assert))))

(t/ann update-worksheet-all-fields [WorksheetEntry Number Number String -> WorksheetEntry])
(defn update-worksheet-all-fields
  "Update all the fields for the given worksheet and return the new worksheet"
  [worksheet-entry rows cols title]
  (let [worksheet (doto ^WorksheetEntry worksheet-entry
                        (.setRowCount (int rows))
                        (.setColCount (int cols))
                        (.setTitle (PlainTextConstruct. title)))]
    (cast WorksheetEntry (doto (.update worksheet)
                           assert))))

(t/ann find-worksheet-by-id
       [SpreadsheetService SpreadsheetEntry String -> (t/U '{:worksheet WorksheetEntry}
                                                           '{:error (t/Val :no-entry)})])
(defn find-worksheet-by-id
  "Given a SpreadsheetService, SpreadSheetEntry and the id of a worksheet, find
   the WorksheetEntry with the given id in a map, or an error message in a map"
  [^SpreadsheetService sheet-service spreadsheet id]
  (let [url (io/as-url (str (.getWorksheetFeedUrl ^SpreadsheetEntry spreadsheet) "/" id))
        etag nil
        entry (tu/ignore-with-unchecked-cast
               (.getEntry sheet-service url WorksheetEntry ^String etag)
               WorksheetEntry)]
    (if entry
      {:worksheet entry}
      {:error :no-entry})))

(t/ann find-worksheet-by-title
       [SpreadsheetService SpreadsheetEntry String -> (t/U '{:worksheet WorksheetEntry}
                                                           '{:error t/Keyword})])
(defn find-worksheet-by-title
  "Given a SpreadsheetService, SpreadSheetEntry and a title of a worksheet, find the WorksheetEntry
   with the given title in a map, or an error message in a map"
  [sheet-service spreadsheet title]
  (let [query (doto (WorksheetQuery. (doto (.getWorksheetFeedUrl ^SpreadsheetEntry spreadsheet)
                                       assert))
                (.setTitleQuery title)
                (.setTitleExact true))
        query-results (doto (.query ^SpreadsheetService sheet-service query WorksheetFeed)
                        assert)
        entries (doto (.getEntries query-results)
                  assert)]
    (cond (= 1 (count entries)) {:worksheet (doto (cast WorksheetEntry (first entries))
                                              assert)}
          (< (count entries) 1) {:error :no-worksheet}
          :else {:error :more-than-one-worksheet})))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;; Editing Data Functions ;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(t/ann find-cell-by-row-col [SpreadsheetService WorksheetEntry Number Number -> (t/U '{:cell CellEntry}
                                                                                     '{:error t/Keyword})])
(defn find-cell-by-row-col
  "Given a SpreadsheetService, a WorksheetEntry, a row and a column,
   return the CellEntry at that location in a map, or an error message in a map"
  [sheet-service worksheet row col]
  (let [row (int row)
        col (int col)
        cell-feed-url (doto (.getCellFeedUrl ^WorksheetEntry worksheet)
                        assert)
        cell-query (doto (CellQuery. cell-feed-url)
                     (.setReturnEmpty true)
                     (.setMinimumRow row)
                     (.setMaximumRow row)
                     (.setMinimumCol col)
                     (.setMaximumCol col))
        query-results (doto (.query ^SpreadsheetService sheet-service cell-query CellFeed)
                        assert)
        cells (doto (.getEntries query-results)
                assert)]
    (cond (= 1 (count cells)) {:cell (doto (cast CellEntry (first cells))
                                       assert)}
          (< (count cells) 1) {:error :no-cells}
          :else {:error :more-than-one-cell})))

(t/ann ^:no-check update-cell
       [cred/GoogleAuth String String '[Number Number String] -> (t/U CellEntry
                                                                     '{:error t/Keyword})])
(defn update-cell
  "Given a google-ctx configuration map, the id of a spreadsheet,
   id of a worksheet in that spreadsheet, and a cell(in form [row col value],
   changes the value in the cell location inside of the given
   worksheet inside of the spreadsheet, or returns an error map"
  [google-ctx spreadsheet-id worksheet-id [row col value]]
  (let [sheet-service ^SpreadsheetService (build-sheet-service google-ctx)
        spreadsheet (find-spreadsheet-by-id sheet-service spreadsheet-id)
        spreadsheet-entry ^SpreadsheetEntry (:spreadsheet spreadsheet)
        worksheet (if (contains? spreadsheet :error)
                    spreadsheet
                    (find-worksheet-by-id sheet-service spreadsheet-entry worksheet-id))
        worksheet-entry ^WorksheetEntry (:worksheet worksheet)
        cell-feed-url (if (contains? worksheet :error)
                        worksheet
                        {:cell-feed-url (.getCellFeedUrl worksheet-entry)})]
    (if (contains? cell-feed-url :error)
      cell-feed-url
      (let [cell-feed-url (:cell-feed-url cell-feed-url)
            cell-id (str "R" row "C" col)
            cell-url (io/as-url (str cell-feed-url "/" cell-id))
            cell (doto (CellEntry. row col value)
                   (.setId (str cell-url)))
            _ (.setHeader sheet-service "If-Match" "*") ]
        (.update sheet-service cell-url cell)))))

(t/ann ^:no-check insert-row
       [cred/GoogleAuth String String (t/Seq (t/Map String String)) -> (t/U ListEntry
                                                                           '{:error t/Keyword})])
(defn insert-row
  "Given a google-ctx configuration map, the name of a spreadsheet,
   name of a worksheet in that spreadsheet, and a map of header-value pairs
   ({header value}) where header and value are both strings.
   NOTE: The headers must be all lowercase with no capital letters even if the header
   in the sheet has either one of those properties
   NOTE: headers are the values in the first row of a Google Spreadsheet"
  [google-ctx spreadsheet-id worksheet-id row-values]
  (let [sheet-service ^SpreadsheetService (build-sheet-service google-ctx)
        spreadsheet (find-spreadsheet-by-id sheet-service spreadsheet-id)
        spreadsheet-entry ^SpreadsheetEntry (:spreadsheet spreadsheet)
        worksheet (if (contains? spreadsheet :error)
                    spreadsheet
                    (find-worksheet-by-id sheet-service spreadsheet-entry worksheet-id))
        worksheet-entry ^WorksheetEntry (:worksheet worksheet)
        list-feed-url (if (contains? worksheet :error)
                        worksheet
                        {:list-feed-url (.getListFeedUrl worksheet-entry)})
        list-feed (if (contains? list-feed-url :error)
                    list-feed-url
                    {:list-feed (.getFeed sheet-service ^URL (:list-feed-url list-feed-url) ListFeed)})
        row (ListEntry.)
        headers (keys row-values)
        update-value-by-header (fn [header]
                                 (.setValueLocal (.getCustomElements row)
                                                 header (get row-values header)))]
    (if (contains? list-feed :error)
      list-feed
      (do (dorun (map update-value-by-header headers))
          (.insert sheet-service (:list-feed-url list-feed-url) row)))))

(t/ann ^:no-check batch-update-cells
       [cred/GoogleAuth String String (t/Seq '[Number Number String]) -> (t/U CellFeed
                                                                             '{:error t/Keyword})])
(defn batch-update-cells
  "Given a google-ctx configuration map, the id of a spreadsheet, the id of
   a worksheet, and a list of cells(in the form [row column value]), sends a batch
   request of all cell updates to the drive api. Will return {:error :msg} if
   something goes wrong along the way"
  [google-ctx spreadsheet-id worksheet-id cells]
  (let [sheet-service ^SpreadsheetService (build-sheet-service google-ctx)
        spreadsheet (find-spreadsheet-by-id sheet-service spreadsheet-id)
        spreadsheet-entry ^SpreadsheetEntry (:spreadsheet spreadsheet)
        worksheet (if (contains? spreadsheet :error)
                    spreadsheet
                    (find-worksheet-by-id sheet-service spreadsheet-entry worksheet-id))
        worksheet-entry ^WorksheetEntry (:worksheet worksheet)
        cell-feed-url (if (contains? worksheet :error)
                        worksheet
                        {:cell-feed-url (.getCellFeedUrl worksheet-entry)})]
    (if (contains? cell-feed-url :error)
      cell-feed-url
      (let [cell-feed-url ^URL (:cell-feed-url cell-feed-url)
            batch-request (CellFeed.)
            create-update-entry (fn [[row col value]]
                                  (let [batch-id (str "R" row "C" col)
                                        entry-url (io/as-url (str cell-feed-url "/" batch-id))
                                        entry (doto (CellEntry. row col value)
                                                (.setId (str entry-url))
                                                (BatchUtils/setBatchId batch-id)
                                                (BatchUtils/setBatchOperationType BatchOperationType/UPDATE))]
                                    (-> batch-request .getEntries (.add entry))))
            update-requests (doall (map create-update-entry cells))
            cell-feed (.getFeed sheet-service cell-feed-url CellFeed)
            batch-link (.getLink cell-feed ILink$Rel/FEED_BATCH ILink$Type/ATOM)
            batch-url (io/as-url (.getHref batch-link))
            _ (.setHeader sheet-service "If-Match" "*")]
        (.batch sheet-service batch-url batch-request)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;; Read and Write entire Worksheets ;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(t/ann ^:no-check read-worksheet-headers [SpreadsheetService WorksheetEntry -> (t/Vec String)])
(defn read-worksheet-headers
  "Given a Spreadsheet Service and a WorksheetEntry, return
   the value of all header cells(the first row in a worksheet)"
  [^SpreadsheetService sheet-service, ^WorksheetEntry worksheet-entry]
  (let [max-col (.getColCount worksheet-entry)
        cell-feed-url (.getCellFeedUrl worksheet-entry)
        cell-query (doto (CellQuery. cell-feed-url)
                     (.setReturnEmpty true)
                     (.setMinimumRow (int 1))
                     (.setMaximumRow (int 1))
                     (.setMinimumCol (int 1))
                     (.setMaximumCol max-col))
        cells (-> (.query sheet-service cell-query CellFeed)
                  .getEntries)
        get-value (fn [^CellEntry cell]
                    (let [value (.getValue (.getCell cell))]
                      (if (string? value) value "")))]
    (into [] (map get-value cells))))

(t/ann ^:no-check read-worksheet-values [SpreadsheetService WorksheetEntry -> (t/Seq (t/Vec String))])
(defn read-worksheet-values
  "Given a SpreadsheetService, and a WorksheetEntry, reads in that worksheet and returns
   the data from the cells as a list of vectors of strings '(['example']). Will return
   {:error :msg} if something goes wrong along the way such as a missing worksheet "
  [^SpreadsheetService sheet-service, ^WorksheetEntry worksheet-entry]
  (let [list-feed (.getFeed sheet-service (.getListFeedUrl worksheet-entry) ListFeed)
        entries (.getEntries list-feed)
        get-value (fn [^CustomElementCollection row, tag]
                    (let [value (.getValue row tag)]
                      (if (string? value) value "")))
        print-value (fn [^ListEntry entry]
                      (let [row (.getCustomElements entry)]
                        (into [] (map #(get-value row %) (.getTags row)))))]
    (map print-value entries)))

(t/ann ^:no-check read-worksheet [cred/GoogleAuth String String -> (t/U '{:headers (t/Vec String)
                                                                         :values (t/Seq (t/Vec String))}
                                                                       '{:error t/Keyword})])
(defn read-worksheet
  "Given a google-ctx configuration map, the id of a spreadsheet, the id of
   a worksheet, reads in the worksheet as a list of vectors of strings, and
   seperates the headers and values of the sheet (first row and all other rows)"
  [google-ctx spreadsheet-id worksheet-id]
  (let [sheet-service (build-sheet-service google-ctx)
        spreadsheet (find-spreadsheet-by-id sheet-service spreadsheet-id)
        worksheet (if (contains? spreadsheet :error)
                    spreadsheet
                    (find-worksheet-by-id sheet-service (:spreadsheet spreadsheet) worksheet-id))]
    (if (contains? worksheet :error)
      worksheet
      (let [headers (read-worksheet-headers sheet-service (:worksheet worksheet))
            values (read-worksheet-values sheet-service (:worksheet worksheet))]
        {:headers headers :values values}))))

(t/ann ^:no-check write-worksheet
       [cred/GoogleAuth String String '{:headers (t/Vec String)
                                      :values (t/Seq (t/Vec String))} -> (t/U '{:error t/Keyword}
                                                                              (t/Seq CellEntry))])
(defn write-worksheet
  "Given a google-ctx configuration map, the id of a spreadsheet, the id of a worksheet,
   and a map of the data {:headers data :values data}, resizes the sheet, which erases all
   of the previous data, creates cells for the new data-map and calls batch-update cells
   on the data in chunks of a certain size that the API can handle"
  [google-ctx spreadsheet-id worksheet-id data-map]
  (let [sheet-service (build-sheet-service google-ctx)
        spreadsheet (find-spreadsheet-by-id sheet-service spreadsheet-id)
        worksheet (if (contains? spreadsheet :error)
                    spreadsheet
                    (find-worksheet-by-id sheet-service (:spreadsheet spreadsheet) worksheet-id))]
    (if (contains? worksheet :error)
      worksheet
      (let [headers (:headers data-map)
            values (:values data-map)
            rows-needed (inc (count values))
            cols-needed (apply max (cons (count headers) (map count values)))
            worksheet-entry ^WorksheetEntry (:worksheet worksheet)
            worksheet-name (.getPlainText (.getTitle worksheet-entry))
            worksheet (update-worksheet-all-fields worksheet-entry
                                                   rows-needed cols-needed worksheet-name)
            build-cell (fn [column value]
                         [(inc column) value])
            build-row (fn [row-number row]
                        (map #(into [] (cons row-number %)) (map-indexed build-cell row)))
            header-cells (map #(vector (inc (first %)) (second %) (nth % 2))
                              (apply concat (map-indexed build-row (list headers))))
            value-cells (map #(vector (+ 2 (first %)) (second %) (nth % 2))
                             (apply concat (map-indexed build-row values)))
            all-cells (partition-all 10000 value-cells)
            all-cells (cons (concat header-cells (first all-cells)) (rest all-cells))]
        (dorun (map #(batch-update-cells google-ctx spreadsheet-id worksheet-id %) all-cells))))))
