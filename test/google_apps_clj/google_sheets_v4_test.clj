(ns google-apps-clj.google-sheets-v4-test
  (:require [clj-time.core :as time]
            [clojure.test :refer :all]
            [google-apps-clj.credentials :as cred]
            [google-apps-clj.google-drive :as gdrive]
            [google-apps-clj.google-drive.mime-types :as mime-types]
            [google-apps-clj.google-sheets-v4 :refer :all])
  (:import [com.google.api.services.sheets.v4.model CellData RowData]))

(deftest test-cell-conversion
  (is (= "foo" (cell->clj "foo")))
  (is (= 2.0 (cell->clj 2.0)))
  (is (= (time/date-time 1950 6 15)
         (cell->clj (time/date-time 1950 6 15))))
  (is (= "foo" (cell->clj (coerce-to-cell "foo"))))
  (is (= 2.0 (cell->clj (coerce-to-cell 2.0M))))
  (is (= ":foo" (cell->clj (coerce-to-cell :foo))))
  (is (= nil (cell->clj (coerce-to-cell nil))))
  (is (= (time/date-time 1950 6 15)
         (cell->clj (coerce-to-cell (time/date-time 1950 6 15)))))
  (is (= 2.01M (cell->clj (currency-cell 2.01M))))
  (is (instance? CellData (cell->clj (formula-cell "A1+B2"))))
  (is (instance? RowData (row->row-data ["foo" nil 2.0])))
  (is (thrown? Exception (coerce-to-cell 299792.457999999984M))))

(deftest ^:integration test-scenario
  (let [creds (cred/default-credential (into scopes
                                             ["https://www.googleapis.com/auth/drive"]))
        filename (name (gensym "google-sheets-v4-test"))
        file (gdrive/upload-file! creds "root" nil filename
                                  {:mime-type mime-types/spreadsheet})
        {:keys [id]} file
        spreadsheet-id (volatile! id)
        sheet-id (volatile! nil)]
    (try
      (let [service (build-service creds)]
        (testing "get-spreadsheet-info"
          (is (get-spreadsheet-info service @spreadsheet-id)))
        (testing "add-sheet"
          (let [sheet (add-sheet service id "new tab")
                {:strs [sheetId title]} sheet]
            (is (= "new tab" title))
            (is sheetId)
            (vreset! sheet-id sheetId)))
        (testing "find-sheet-id"
          (is (= @sheet-id
                 (find-sheet-id service @spreadsheet-id "new tab")))
          (is (= nil
                 (find-sheet-id service @spreadsheet-id "no such tab"))))
        (testing "obtain-sheet-id"
          (is (= @sheet-id (obtain-sheet-id service @spreadsheet-id "new tab")))
          (let [new-sheet-id (obtain-sheet-id service @spreadsheet-id "another tab")]
            (is new-sheet-id)
            (is (not= @sheet-id new-sheet-id))
            (is (= new-sheet-id (find-sheet-id service @spreadsheet-id "another tab")))))
        (testing "write-sheet"
          (let [rows [(mapv (comp str char) (range (int \A) (inc (int \Z))))
                      (into [] (repeat 26 0))]]
            (is (nil? (write-sheet service @spreadsheet-id @sheet-id rows)))))
        (testing "append-sheet"
          (let [rows [(into [] (repeat 26 "test"))]
                response (append-sheet service @spreadsheet-id @sheet-id rows)]
            (is (= 1 (count response)))
            (is (= @spreadsheet-id (get (first response) "spreadsheetId")))))
        (testing "get-cells"
          (let [cells (get-cells service @spreadsheet-id ["new tab!A1:Z3"])]
            (is (= 1 (count cells)))
            (is (= 3 (count (first cells))))
            (is (apply = 26 (map count (first cells))))))
        (testing "get-cell-values"
          (testing "entire range, specified"
            (let [data (get-cell-values service @spreadsheet-id ["new tab!A1:Z3"])]
              (is (= [[(mapv (comp str char) (range (int \A) (inc (int \Z))))
                       (into [] (repeat 26 0.0))
                       (into [] (repeat 26 "test"))]]
                     data))))
          (testing "subrange, specified"
            (let [data (get-cell-values service @spreadsheet-id ["new tab!A1:A5"])]
              (is (= [[["A"] [0.0] ["test"]]]
                     data))))
          (testing "column range"
            (let [data (get-cell-values service @spreadsheet-id ["new tab!A:A"])]
              (is (= [[["A"] [0.0] ["test"]]] data))))
          (testing "row range"
            (let [data (get-cell-values service @spreadsheet-id ["new tab!1:1"])]
              (is (= [[(mapv (comp str char) (range (int \A) (inc (int \Z))))]]
                     data))))
          (testing "partial column range"
            (let [data (get-cell-values service @spreadsheet-id ["new tab!A2:A"])]
              (is (= [[[0.0] ["test"]]] data))))
          (testing "partial row range"
            (let [data (get-cell-values service @spreadsheet-id ["new tab!B2:2"])]
              (is (= [[(into [] (repeat 25 0.0))]] data))))
          (testing "entire range"
            (let [data (get-cell-values service @spreadsheet-id ["new tab"])]
              (is (= [[(mapv (comp str char) (range (int \A) (inc (int \Z))))
                       (into [] (repeat 26 0.0))
                       (into [] (repeat 26 "test"))]]
                     data))))
          (testing "specific range from the (implicit) first sheet"
            (let [data (get-cell-values service @spreadsheet-id ["A1:A2"])]
              (is (= [[]] data))))))
      (finally
        (gdrive/delete-file! creds id)))))
