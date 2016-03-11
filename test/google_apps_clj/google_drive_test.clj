(ns google-apps-clj.google-drive-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer :all]
            [google-apps-clj.google-drive :refer :all]
            [google-apps-clj.credentials :as gauth]))

(deftest ^:integration test-scenario
  (let [creds (gauth/default-credential ["https://www.googleapis.com/auth/drive"])
        q! (partial execute-query! creds)
        folder-name (name (gensym "google-apps-clj-google-drive-"))
        folder (create-folder! creds "root" folder-name)
        folder-id (:id folder)
        upload-content (.getBytes "test-body" "UTF-8")
        upload-request (file-insert-query folder-id upload-content "test-title"
                                          {:description "test-description"
                                           :mime-type "text/plain"})]
    (try
      (testing "creates a folder"
        (is folder-id)
        (is (= folder-name (:title folder)))
        (is (folder? folder)))
      (testing "uploads a file"
        (let [file (q! upload-request)
              file-id (:id file)]
          (is file-id)
          (is (= "test-title" (:title file)))
          (is (= "test-description" (:description file)))
          (testing "converts files when possible"
            (is (= "application/vnd.google-apps.document" (:mime-type file))))
          (let [file' (get-file! creds file-id)]
            (is (= "test-title" (:title file')))
            (is (= "test-description" (:description file')))
            (is (= "application/vnd.google-apps.document" (:mime-type file'))))
          (let [files (list-files! creds folder-id)]
            (is (= [file-id] (map :id files))))
          (delete-file! creds file-id)))
      (testing "file permissions"
        (is (= [["owner" "user"]]
               (map (juxt :role :type) (get-permissions! creds folder-id))))
        (let [file-id (:id (q! upload-request))]
          (testing "newly created files have only the owner permission"
            (is (= [["owner" "user"]]
                   (map (juxt :role :type) (get-permissions! creds file-id)))))
          (testing "managing authorization"
            (assign! creds file-id {:principal "dev@sparkfund.co"
                                    :role :reader ;TODO: figure out why Google rejects "reader" role
                                    :searchable? false})
            (is (= [["owner" "user"]
                    ["reader" "group"]]
                   (map (juxt :role :type) (get-permissions! creds file-id))))
            (revoke! creds file-id "dev@sparkfund.co")
            (Thread/sleep 5000)
            (is (= [["owner" "user"]]
                   (map (juxt :role :type) (get-permissions! creds file-id)))))))
      (finally
        (delete-file! creds folder-id)))))
