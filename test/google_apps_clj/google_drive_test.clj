(ns google-apps-clj.google-drive-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer :all]
            [google-apps-clj.google-drive :refer :all]))

(deftest ^:integration test-scenario
  (let [creds (edn/read-string (slurp "google-creds.edn"))
        q! (partial execute-query! creds)
        folder-name (name (gensym "google-apps-clj-google-drive-"))
        folder (create-folder! creds "root" folder-name)
        folder-id (:id folder)
        upload-request (upload-file folder-id
                                    "test-title"
                                    "test-description"
                                    "text/plain"
                                    (.getBytes "test-body" "UTF-8"))]
    (try
      (testing "created a folder"
        (is folder-id)
        (is (= folder-name (:title folder)))
        (is (folder? folder)))
      (testing "uploads a file"
        (let [file (execute-query! creds upload-request)
              file-id (:id file)]
          (is file-id)
          (is (= "test-title" (:title file)))
          (is (= "test-description" (:description file)))
          (is (= "text/plain" (:mime-type file)))
          (let [file' (get-file! creds file-id)]
            (is (= "test-title" (:title file')))
            (is (= "test-description" (:description file')))
            (is (= "text/plain" (:mime-type file'))))
          (let [files (list-files! creds folder-id)]
            (is (= [file-id] (map :id files))))
          (delete-file! creds file-id)))
      (testing "file permissions"
        (testing "in an unshared folder"
          (let [folder (q! (-> (get-file folder-id)
                               (with-fields [:permissions])))
                {:keys [permissions]} folder]
            (is (= [["owner" "user"]]
                   (map (juxt :role :type) permissions)))))
        (let [file (q! (-> upload-request
                           (with-fields [:id :permissions])))
              {:keys [id permissions]} file]
          (is (= [["owner" "user"]]
                 (map (juxt :role :type) permissions)))
          (assign! creds {:file-id id
                          :principal "dev@sparkfund.co"
                          :role :reader})
          (let [file (q! (-> (get-file id)
                             (with-fields [:permissions])))
                {:keys [permissions]} file]
            (is (= [["owner" "user"]
                    ["reader" "group"]]
                   (map (juxt :role :type) permissions))))))
      (finally
        (delete-file! creds folder-id)))))
