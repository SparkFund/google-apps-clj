(ns google-apps-clj.google-drive-test
  (:require [clojure.edn :as edn]
            [clojure.test :refer :all]
            [google-apps-clj.google-drive :refer :all]))

(deftest ^:integration test-scenario
  (let [creds (assoc (edn/read-string (slurp "google-creds.edn"))
                     :read-timeout 60000)
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
      (prn "test run" folder-name)
      (testing "created a folder"
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
        (testing "in an unshared folder"
          (is (= [["owner" "user"]]
                 (map (juxt :role :type) (get-permissions! creds folder-id))))
          (let [file-id (:id (q! upload-request))]
            (testing "newly created files have only the owner permission"
              (is (= [["owner" "user"]]
                     (map (juxt :role :type) (get-permissions! creds file-id)))))
            (testing "managing authorization"
              (assign! creds file-id {:principal "dev@sparkfund.co"
                                      :role :reader
                                      :searchable? false})
              (Thread/sleep 5000)
              (is (= [["owner" "user"]
                      ["reader" "group"]]
                     (map (juxt :role :type) (get-permissions! creds file-id))))
              (assign! creds file-id {:principal "dev@sparkfund.co"
                                      :role :writer
                                      :searchable? false})
              (Thread/sleep 5000)
              (is (= [["owner" "user"]
                      ["writer" "group"]]
                     (map (juxt :role :type) (get-permissions! creds file-id))))
              (revoke! creds file-id "dev@sparkfund.co")
              (Thread/sleep 5000)
              (is (= [["owner" "user"]]
                     (map (juxt :role :type) (get-permissions! creds file-id)))))))
        (testing "in a shared folder"
          (assign! creds folder-id {:principal "sparkfund.co"
                                    :role :writer
                                    :searchable? true})
          (Thread/sleep 5000)
          (testing "files inherit their folder's permissions"
            (let [file-id (:id (q! upload-request))]
              (prn "file" file-id)
              (is (= [["owner" "user"]
                      ["writer"  "domain"]]
                     (map (juxt :role :type) (get-permissions! creds file-id))))
              (testing "and their changes"
                (assign! creds folder-id {:principal "sparkfund.co"
                                          :role :reader
                                          :searchable? true})
                (Thread/sleep 30000)
                (prn "checking folder")
                (is (= [["owner" "user"]
                        ["reader"  "domain"]]
                       (map (juxt :role :type) (get-permissions! creds folder-id))))
                (prn "checking file")
                (is (= [["owner" "user"]
                        ["reader"  "domain"]]
                       (map (juxt :role :type) (get-permissions! creds file-id))))))
            (let [file-id (:id (q! upload-request))]
              (is (= [["owner" "user"]
                      ["reader" "domain"]]
                     (map (juxt :role :type) (get-permissions! creds file-id))))
              (testing "files retain their folder's permissions when they assign"
                (assign! creds file-id {:principal "dev@sparkfund.co"
                                        :role :writer
                                        :searchable? true})
                (Thread/sleep 5000)
                (is (= [["owner" "user"]
                        ["reader" "domain"]
                        ["writer" "group"]]
                       (map (juxt :role :type) (get-permissions! creds file-id)))))))))
      (finally
        #_(delete-file! creds folder-id)))))
