(ns google-apps-clj.google-drive
  "A library for connecting to Google Drive through the Drive API"
  (:require [clojure.core.typed :as t]
            [clojure.edn :as edn :only [read-string]]
            [clojure.java.io :as io :only [as-url file resource]]
            [google-apps-clj.credentials :as cred])
  (:import (com.google.api.client.http FileContent
                                       GenericUrl)
           (com.google.api.services.drive Drive
                                          Drive$Builder
                                          DriveScopes)
           (com.google.api.services.drive.model File
                                                ParentReference)))

(t/ann ^:no-check clojure.core/slurp [java.io.InputStream -> String])

(t/ann build-drive-service [cred/GoogleCtx -> Drive])
(defn build-drive-service
  "Given a google-ctx configuration map, builds a Drive service using 
   credentials coming from the OAuth2.0 credential setup inside google-ctx"
  [google-ctx]
  (let [drive-builder (->> google-ctx
                           cred/build-credential
                           (Drive$Builder. cred/http-transport cred/json-factory))]
    (cast Drive (doto (.build drive-builder)
                  assert))))

(t/ann upload-file [cred/GoogleCtx java.io.File String String String String -> File])
(defn upload-file
  "Given a google-ctx configuration map, a file to upload, an ID of 
   the parent folder you wish to insert the file in, the title of the 
   Drive file, the description of the Drive file, and the MIME type of
   the file, builds a Drive Service and inserts this file into Google
   Drive with permissions of the folder it's inserted into. The owner
   is whomever owns the Credentials used to make the Drive Service"
  [google-ctx file parent-folder-id file-title file-description media-type]
  (let [drive-service (build-drive-service google-ctx)
        parent-folder (doto (ParentReference.)
                        (.setId parent-folder-id))
        drive-file (doto (File.)
                     (.setTitle file-title)
                     (.setDescription file-description)
                     (.setMimeType media-type)
                     (.setParents (vector parent-folder)))
        media-content (FileContent. media-type file)
        drive-files (doto (.files ^Drive drive-service)
                     assert)
        drive-file (doto (.insert drive-files drive-file media-content)
                     assert
                     (.setConvert true))]
    (cast File (doto (.execute drive-file)
                 assert))))

(t/ann create-blank-file [cred/GoogleCtx String String String String -> File])
(defn create-blank-file
  "Given a google-ctx configuration map, an ID of the parent folder you
   wish to insert the file in, the title of the Drive file, the description
   of the Drive file, and the MIME type of the file(which will be converted 
   into a google file type, builds a Drive Service and inserts a blank file 
   into Google Drive with permissions of the folder it's inserted into. The 
   owner is whomever owns the Credentials used to make the Drive Service"
  [google-ctx parent-folder-id file-title file-description media-type]
  (let [file (doto (java.io.File/createTempFile "temp" "temp")
               assert)]
    (upload-file google-ctx file parent-folder-id file-title file-description media-type)))


(t/ann update-file-title [cred/GoogleCtx String String -> File])
(defn update-file-title
  "Given a google-ctx configuration map, a file id, and a title,
   updates the title of the given file to the given title."
  [google-ctx file-id title]
  (let [drive-service (build-drive-service google-ctx)
        files (doto (.files ^Drive drive-service)
                assert)
        files-get (doto (.get files file-id)
                    assert)
        file (cast File (doto (.execute files-get)
                          assert))
        file (doto (.setTitle ^File file title)
               assert)
        update-request (doto (.update files file-id file)
                         assert)]
    (cast File (doto (.execute update-request)
                 assert))))

(t/ann update-file-description [cred/GoogleCtx String String -> File])
(defn update-file-description
  "Given a google-ctx configuration map, a file id, and a description,
   updates the description of the given file to the given description."
  [google-ctx file-id description]
  (let [drive-service (build-drive-service google-ctx)
        files (doto (.files ^Drive drive-service)
                assert)
        files-get (doto (.get files file-id)
                    assert)
        file (cast File (doto (.execute files-get)
                          assert))
        file (doto (.setDescription ^File file description)
               assert)
        update-request (doto (.update files file-id file)
                         assert)]
    (cast File (doto (.execute update-request)
                 assert))))

(t/ann download-file [cred/GoogleCtx String String -> String])
(defn download-file
  "Given a google-ctx configuration map, a file id to download, 
   and a media type, download the drive file and then read it in 
   and return the result of reading the file"
  [google-ctx file-id media-type]
  (let [drive-service (build-drive-service google-ctx)
        files (doto (.files ^Drive drive-service)
                assert)
        files-get (doto (.get files file-id)
                    assert)
        file (cast File (doto (.execute files-get)
                          assert))
        http-request (doto (.getRequestFactory ^Drive drive-service)
                       assert)
        export-link (doto (.getExportLinks ^File file)
                      assert)
        generic-url (GenericUrl. ^String (doto (cast String (get export-link media-type))
                                                         assert))
        get-request (doto (.buildGetRequest http-request generic-url)
                      assert)
        response (doto (.execute get-request)
                   assert)
        input-stream (doto (.getContent response)
                       assert)]
    (slurp input-stream)))

