(ns google-apps-clj.google-drive
  "A library for connecting to Google Drive through the Drive API"
  (:require [clojure.java.io :as io :only [file resource as-url]]
            [google-apps-clj.credentials :as cred])
  (:import (com.google.api.services.drive Drive$Builder
                                          DriveScopes)
           (com.google.api.services.drive.model File
                                                ParentReference)
           (com.google.api.client.http FileContent
                                       GenericUrl)))

(defn build-drive-service
  "Given a google-ctx configuration map, builds a Drive service using 
   credentials coming from the OAuth2.0 credential setup inside google-ctx"
  [google-ctx]
  (->> google-ctx
       cred/build-credential
       (Drive$Builder. cred/http-transport cred/json-factory)
       .build))

(defn upload-file!
  "Given a google-ctx configuration map, a file to upload, an ID of 
   the parent folder you wish to insert the file in, the title of the 
   Drive file, the description of the Drive file, and the MIME type of
   the file, builds a Drive Service and inserts this file into Google
   Drive with permissions of the folder it's inserted into. The owner
   is whomever owns the Credentials used to make the Drive Service"
  [google-ctx file parent-folder-id file-title file-description media-type]
  (let [drive-service (build-drive-service google-ctx)
        parent-folder (-> (ParentReference.)
                          (.setId parent-folder-id)
                          vector)
        drive-file (-> (File.)
                       (.setTitle file-title)
                       (.setDescription file-description)
                       (.setMimeType media-type)
                       (.setParents parent-folder))
        media-content (FileContent. media-type file)
        drive-file-metadata (-> (.files drive-service)
                                (.insert drive-file media-content)
                                (.setConvert true)
                                .execute)]
    drive-file-metadata))

(defn download-file!
  "Given a google-ctx configuration map, a file name to download, 
   and the MIME type you wish to export it as, download the drive file
   and then read it in and return the result of reading the file or
   an error message if the file is not found or if there are too many"
  [google-ctx file-name media-type]
  (let [drive-service (build-drive-service google-ctx)
        files (-> (.files drive-service)
                  .list
                  (.setQ (str "title = '" file-name "'"))
                  .execute
                  .getItems)
        file (cond (= (count files) 1) {:file (first files)}
                   (< (count files) 1) {:error :no-file}
                   (> (count files) 1) {:error :more-than-one-file})] 
    (if (contains? file :error)
      file
      (-> drive-service
          .getRequestFactory
          (.buildGetRequest (-> file :file .getExportLinks (get media-type) GenericUrl.))
          .execute
          .getContent
          slurp))))
