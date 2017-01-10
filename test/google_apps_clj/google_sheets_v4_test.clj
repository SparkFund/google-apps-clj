(ns google-apps-clj.google-sheets-v4-test
  (:require [clj-time.core :as time]
            [clojure.test :refer :all]
            [google-apps-clj.credentials :as cred]
            [google-apps-clj.google-drive :as gdrive]
            [google-apps-clj.google-drive.mime-types :as mime-types]
            [google-apps-clj.google-sheets-v4 :refer :all])
  (:import [com.google.api.services.sheets.v4.model CellData RowData]))

(deftest test-cell-data-conversion
  (is (= "foo" (cell-data->clj "foo")))
  (is (= 2.0 (cell-data->clj 2.0)))
  (is (= (time/date-time 1950 6 15)
         (cell-data->clj (time/date-time 1950 6 15))))
  (is (= "foo" (cell-data->clj (coerce-to-cell-data "foo"))))
  (is (= 2.0 (cell-data->clj (coerce-to-cell-data 2.0M))))
  (is (= ":foo" (cell-data->clj (coerce-to-cell-data :foo))))
  (is (= nil (cell-data->clj (coerce-to-cell-data nil))))
  (is (= (time/date-time 1950 6 15)
         (cell-data->clj (coerce-to-cell-data (time/date-time 1950 6 15)))))
  (is (= 2.01M (cell-data->clj (currency-cell-data 2.01M))))
  (is (instance? CellData (cell-data->clj (formula-cell-data "A1+B2"))))
  (is (instance? RowData (row->row-data ["foo" nil 2.0])))
  (is (thrown? Exception (coerce-to-cell-data 299792.457999999984M))))

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
                      (into [] (repeat 26 0))]
                response (write-sheet service @spreadsheet-id @sheet-id rows)]
            (is (= 1 (count response)))
            (is (= @spreadsheet-id (get (first response) "spreadsheetId")))))
        (testing "append-sheet"
          (let [rows [(into [] (repeat 26 "test"))]
                response (append-sheet service @spreadsheet-id @sheet-id rows)]
            (is (= 1 (count response)))
            (is (= @spreadsheet-id (get (first response) "spreadsheetId")))))
        (testing "get-effective-vals"
          (let [data (get-effective-vals service @spreadsheet-id ["new tab!A1:Z3"])]
            (is (= [[(mapv (comp str char) (range (int \A) (inc (int \Z))))
                     (into [] (repeat 26 0.0))
                     (into [] (repeat 26 "test"))]]
                   data)))
          (let [data (get-effective-vals service @spreadsheet-id ["new tab!A1:A5"])]
            (is (= [[["A"] [0.0] ["test"]]]
                   data)))))
      (finally
        (gdrive/delete-file! creds id)))))
