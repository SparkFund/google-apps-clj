(ns google-apps-clj.google-drive-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer :all]
            [google-apps-clj.google-drive :refer :all]))

(deftest ^:integration test-scenario
  (let [creds (edn/read-string (slurp "google-creds.edn"))
        folder-name (name (gensym "google-apps-clj-google-drive-"))
        folder (create-folder! creds "root" folder-name)
        folder-id (:id folder)]
    (is folder-id)
    (is (= folder-name (:title folder)))
    (is (folder? folder))
    (try
      (let [file (upload-file! creds
                               folder-id
                               "test-title"
                               "test-description"
                               "text/plain"
                               (.getBytes "test-body" "UTF-8"))
            file-id (:id file)]
        (is file-id)
        (is (= "test-title" (:title file)))
        (is (= "test-description" (:description file)))
        (is (= "text/plain" (:mime-type file)))
        (let [file' (get-file! creds file-id)]
          (is (= "test-title" (:title file')))
          (is (= "test-description" (:description file')))
          (is (= "text/plain" (:mime-type file'))))
        (delete-file! creds file-id))
      (finally
        (delete-file! creds folder-id)))))
