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
  (is (= 2.01M (cell-data->clj (with-money-format 2.01M))))
  (is (instance? CellData (cell-data->clj (formula "A1+B2"))))
  (is (instance? RowData (row->row-data ["foo" nil 2.0]))))

(deftest ^:integration test-scenario
  (let [creds (cred/default-credential (into scopes
                                             ["https://www.googleapis.com/auth/drive"]))
        filename (name (gensym "google-sheets-v4-test"))
        file (gdrive/upload-file! creds "root" nil filename
                                  {:mime-type mime-types/spreadsheet})
        {:keys [id]} file]
    (try
      (let [service (build-service creds)]
        (testing "get-sheet-info"
          (is (get-sheet-info service id))))
      (finally
        (gdrive/delete-file! creds id)))))
