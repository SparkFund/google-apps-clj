(ns google-apps-clj.google-sheets-v4-test
  (:require [clojure.test :refer :all]
            [google-apps-clj.credentials :as cred]
            [google-apps-clj.google-drive :as gdrive]
            [google-apps-clj.google-drive.mime-types :as mime-types]
            [google-apps-clj.google-sheets-v4 :refer :all]))

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
