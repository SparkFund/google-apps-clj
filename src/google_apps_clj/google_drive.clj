(ns google-apps-clj.google-drive
  "A library for connecting to Google Drive through the Drive API"
  (:require [clojure.core.typed :as t]
            [clojure.core.typed.unsafe :as tu]
            [clojure.edn :as edn :only [read-string]]
            [clojure.java.io :as io :only [as-url file resource]]
            [clojure.string :as string]
            [google-apps-clj.credentials :as cred])
  (:import (com.google.api.client.googleapis.batch BatchRequest
                                                   BatchCallback)
           (com.google.api.client.googleapis.json GoogleJsonErrorContainer)
           (com.google.api.client.http FileContent
                                       InputStreamContent
                                       GenericUrl)
           (com.google.api.client.util GenericData)
           (com.google.api.services.drive Drive
                                          Drive$Builder
                                          Drive$Files$Get
                                          Drive$Files$Insert
                                          Drive$Files$List
                                          Drive$Files$Update
                                          Drive$Permissions$Delete
                                          Drive$Permissions$GetIdForEmail
                                          Drive$Permissions$Insert
                                          Drive$Permissions$List
                                          Drive$Permissions$Update
                                          DriveRequest
                                          DriveScopes)
           (com.google.api.services.drive.model File
                                                FileList
                                                ParentReference
                                                Permission
                                                PermissionId
                                                PermissionList
                                                Property
                                                PropertyList)
           (java.io InputStream)))

(t/ann ^:no-check clojure.core/slurp [java.io.InputStream -> String])

(t/ann build-drive-service [cred/GoogleCtx -> Drive])
(defn ^Drive build-drive-service
  "Given a google-ctx configuration map, builds a Drive service using
   credentials coming from the OAuth2.0 credential setup inside google-ctx"
  [google-ctx]
  (let [drive-builder (->> google-ctx
                           cred/build-credential
                           (Drive$Builder. cred/http-transport cred/json-factory))]
    (cast Drive (doto (.build drive-builder)
                  assert))))

;;; Experimental fns operating on query data structures, with support for batching

(t/defalias Query
  '{:type Keyword
    :action Keyword
    :fields '[Keyword]
    :query (t/Maybe String)
    :file-id (t/Maybe String)})

(t/defalias Request
  (t/U Drive$Files$Get
       Drive$Files$Insert
       Drive$Files$List
       Drive$Files$Update
       Drive$Permissions$GetIdForEmail
       Drive$Permissions$Insert
       Drive$Permissions$List
       Drive$Permissions$Update))

(defn ^File build-file
  [query]
  (let [{:keys [title description writers-can-share?]} query]
    (cond-> (File.)
      title (.setTitle title)
      description (.setDescription description)
      (not (nil? writers-can-share?)) (.setWritersCanShare writers-can-share?))))

(defn ^InputStreamContent build-stream
  [query]
  (let [{:keys [content type size]} query]
    (when content
      (cond-> (InputStreamContent. ^String type (io/input-stream content))
        size (.setLength ^Long size)))))

(t/ann build-request [cred/GoogleCtx Query -> Request])
(defn ^DriveRequest build-request
  "Converts a query into a stateful request object executable in the
   given google context. Queries are maps with the following required
   fields:

   :model - :files, :permissions
   :action - :list, :get, :update, :insert, :delete

   Other fields may be given, and may be required by the action and model:

   :fields - a seq of keywords specifying the object projection
   :query - used to constrain a list of files
   :file-id - specifies the file for file-specific models and actions"
  [google-ctx query]
  (let [drive (build-drive-service google-ctx)
        {:keys [model action fields]} query
        ;; TODO more rigorous support for nesting, e.g. permissions(role,type)
        fields (when (seq fields) (string/join "," (map name fields)))
        items? (= :list action)
        fields-seq (cond-> []
                     (and items? (= model :files))
                     (conj "nextPageToken")
                     (and items? fields)
                     (conj (format "items(%s)" fields))
                     (and items? (not fields))
                     (conj "items")
                     (and (not items?) fields)
                     (conj fields))
        fields (when (seq fields-seq) (string/join "," fields-seq))]
    (case model
      :files
      (case action
        :list
        (let [{:keys [query]} query]
          (cond-> (.list (.files drive))
            fields (.setFields fields)
            query (.setQ query)))
        :get
        (let [{:keys [file-id]} query]
          (cond-> (.get (.files drive) file-id)
            fields (.setFields fields)))
        :update
        (let [{:keys [file-id]} query
              file (build-file query)
              stream (build-stream query)
              request (if stream
                        (.update (.files drive) file-id file stream)
                        (.update (.files drive) file-id file))]
          (cond-> request
            fields (.setFields fields)))
        :insert
        (let [file (build-file query)
              stream (build-stream query)
              request (if stream
                        (.insert (.files drive) file stream)
                        (.insert (.files drive) file))]
          (cond-> request
            fields (.setFields fields))))
      :permissions
      (case action
        :list
        (let [{:keys [file-id]} query]
          (cond-> (.list (.permissions drive) file-id)
            fields (.setFields fields)))
        :insert
        (let [{:keys [file-id value role type with-link?]} query
              permission (-> (Permission.)
                             (.setRole (name role))
                             (.setType (name type))
                             (.setValue value)
                             (cond-> (not (nil? with-link?))
                               (.setWithLink with-link?)))]
          (-> (.insert (.permissions drive) file-id permission)
              (.setSendNotificationEmails false)
              (cond-> fields (.setFields fields))))
        ;; TODO this is of debatable utility since it doesn't work for domain or
        ;; anyone principals, and the id has no stability guarantee anyway
        :get-id-for-email
        (let [{:keys [email]} query]
          (.getIdForEmail (.permissions drive) email))
        :update
        (let [{:keys [file-id permission-id role transfer-ownership?]} query
              permission (-> (Permission.)
                             (.setRole role))]
          (cond-> (.update (.permissions drive) file-id permission-id permission)
            (not (nil? transfer-ownership?)) (.setTransferOwnership transfer-ownership?)
            fields (.setFields fields)))
        :delete
        (let [{:keys [file-id permission-id]} query]
          (-> (.delete (.permissions drive) file-id permission-id)))))))

(defprotocol Requestable
  (response-data
   [request response]
   "Extracts the good bit from the response")
  (next-page!
   [request response]
   "Mutates the request to retrieve the next page of results if supported and
    present"))

(extend-protocol Requestable
  Drive$Files$List
  (next-page! [request ^FileList response]
    (when-let [page-token (.getNextPageToken response)]
      (.setPageToken request page-token)))
  (response-data [request ^FileList response]
    (.getItems response))

  Drive$Files$Get
  (next-page! [request response])
  (response-data [request response]
    response)

  Drive$Files$Insert
  (next-page! [request response])
  (response-data [request response]
    response)

  Drive$Files$Update
  (next-page! [request response])
  (response-data [request response]
    response)

  Drive$Permissions$Delete
  (next-page! [request response])
  (response-data [request response]
    response)

  Drive$Permissions$GetIdForEmail
  (next-page! [request response])
  (response-data [request response]
    response)

  Drive$Permissions$Insert
  (next-page! [request response])
  (response-data [request response]
    response)

  Drive$Permissions$List
  (next-page! [request response])
  (response-data [request ^PermissionList response]
    (.getItems response))

  Drive$Permissions$Update
  (next-page! [request response])
  (response-data [request response]
    response)

  )

(defn execute-query!
  "Executes the given query in the google context and returns the results.
   If the response is paginated, all results are fetched and concatenated
   into a vector."
  [google-ctx query]
  (let [request (build-request google-ctx query)
        results (atom nil)]
    ; TODO the results could be a volatile in clojure 1.7
    (loop []
      (let [response (.execute request)
            data (response-data request response)]
        (if (next-page! request response)
          (do
            (swap! results (fn [extant] (into (or extant []) data)))
            (recur))
          (swap! results (fn [extant]
                           (if extant
                             (into extant data)
                             data))))))
    @results))

(defn execute-batch!
  "Execute the given queries in a batch, returning their responses in
   the same order as the queries. If any queries in a batch yield
   paginated responses, another batch will be executed for all such
   queries, iteratively until all pages have been received, and the
   results concatenated into vectors as in execute!."
  [google-ctx queries]
  ;; TODO partition queries into batches of 1000
  (let [requests (map (partial build-request google-ctx) queries)
        credential (cred/build-credential google-ctx)
        batch (BatchRequest. cred/http-transport credential)
        responses (transient (into [] (repeat (count requests) nil)))]
    (loop [requests (map-indexed vector requests)]
      (let [next-requests (transient {})]
        (doseq [[i ^DriveRequest request] requests]
          (.queue request batch GoogleJsonErrorContainer
                  (reify BatchCallback
                    (onSuccess [_ response headers]
                      (let [data (response-data request response)
                            extant (nth responses i)]
                        (if (next-page! request response)
                          (let [response (into (or extant []) data)]
                            (assoc! next-requests i request)
                            (assoc! responses i response))
                          (let [response (if extant (into extant data) data)]
                            (assoc! responses i response)))))
                    (onFailure [_ error headers]
                      (assoc! responses i error)))))
        (.execute batch)
        (let [next-requests (persistent! next-requests)]
          (when (seq next-requests)
            (recur next-requests)))))
    (persistent! responses)))

(defn execute!
  "Executes the given queries in the most efficient way"
  [google-ctx queries]
  (when (seq queries)
    (if (= 1 (count queries))
      [(execute-query! google-ctx (first queries))]
      (execute-batch! google-ctx queries))))

(defn derive-type
  [principal]
  (cond (= "anyone" principal)
        :anyone
        (pos? (.indexOf principal "@"))
        :user ; This seems to work correctly for users and groups
        :else
        :domain))

(defn find-extant-permissions
  [google-ctx file-id principal]
  (let [list-query {:model :permissions
                    :action :list
                    :file-id file-id
                    :fields [:id :role :withLink :type :domain :emailAddress]}]
    (->> (execute-query! google-ctx list-query)
         (filter (fn [permission]
                   (condp = (derive-type principal)
                     :user
                     (and (= principal (get permission "emailAddress"))
                          (#{"user" "group"} (get permission "type")))
                     :domain
                     (and (= principal (get permission "domain"))
                          (= "domain" (get permission "type")))
                     :anyone
                     (and (= "anyone" (get permission "type")))))))))

(defn assign!
  "Authorize the principal with the role on the given file. The principal will
   not be able to discover the file via google unless the searchable? field is
   true. The principal may be the literal \"anyone\", an email address of a
   user or google app group, or a google app domain.

   If the principal has any other permissions, they will be deleted. If the
   principal has permission for this authorization already, it will be left
   intact, otherwise a new permission will be inserted.

   This operation should be idempotent until the the permissions change by some
   other operation."
  [google-ctx authorization]
  (let [{:keys [file-id principal role searchable?]} authorization
        extant (find-extant-permissions google-ctx file-id principal)
        found (atom false) ; TODO this could be a volatile
        ids-to-delete (transient [])]
    ;; [principal withLink] seem to be a unique key within a file
    (doseq [permission extant]
      (if (and (= (name role) (get permission "role"))
               (case searchable?
                 true (true? (get permission "withLink"))
                 false (nil? (get permission "withLink"))))
        (reset! found true)
        (conj! ids-to-delete (get permission "id"))))
    (let [deletes (mapv (fn [id] {:model :permissions
                                  :action :delete
                                  :file-id file-id
                                  :permission-id id})
                        (persistent! ids-to-delete))
          insert (when-not @found
                   {:model :permissions
                    :action :insert
                    :file-id file-id
                    :value principal
                    :role role
                    :type (derive-type principal)
                    :with-link? searchable?
                    :fields [:id]})
          queries (cond-> deletes
                    insert (conj insert))]
      (execute! google-ctx queries))
    nil))

(defn revoke!
  "Revoke all permissions for the given principal on the given file. The
   principal may be the literal \"anyone\", an email address of a user or
   google app group, or a google app domain.

   This operation should be idempotent until the the permissions change by some
   other operation."
  [google-ctx deauthorization]
  (let [{:keys [file-id principal]} deauthorization
        extant (find-extant-permissions google-ctx file-id principal)
        deletes (mapv (fn [permission]
                        {:model :permissions
                         :action :delete
                         :file-id file-id
                         :permission-id (get permission "id")})
                      extant)]
    (execute! google-ctx deletes)
    nil))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; File Management ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(t/ann ^:no-check get-file-ids [cred/GoogleCtx -> (t/Map String String)])
(defn get-file-ids
  "Given a google-ctx configuration map, gets the file-id and title
   for every file under the users Drive as a map in the structure
   of {file-id file-title}"
  [google-ctx]
  (let [drive-service (build-drive-service google-ctx)
        drive-files (doto (.files ^Drive drive-service)
                      assert)
        files-list (doto (.list drive-files)
                     assert)
        all-files (doto (.getItems (.execute files-list))
                    assert)
        extract-id (fn [file]
                     (let [file-map (into {} file)]
                       {(get file-map "id") (get file-map "title")}))]
    (into {} (map extract-id all-files))))

(t/ann query-files [cred/GoogleCtx String -> (t/Vec File)])
(defn query-files
  "Runs the given query against the given context and returns the results
   as a vector of File objects"
  [google-ctx query]
  ;; The Drive object explicitly disclaims thread-safety, and the contracts
  ;; given by the execute response and items are unclear, so instead of
  ;; concatenating the items, we explicitly copy them into a vector.
  ;;
  ;; We eagerly realize the results to avoid the stack abuse given by the naive
  ;; lazy seq recursive concat approach, as well as to reduce the chance of
  ;; drive mutations affecting the results.
  (let [request (some-> (build-drive-service google-ctx)
                        .files
                        .list
                        (.setQ query))
        results (transient [])]
    request
    #_(loop []
        (let [response (.execute request)]
          (doseq [file (.getItems response)]
            (conj! results file))
          (when-let [page-token (.getNextPageToken response)]
            (.setPageToken request page-token)
            (recur))))
    #_(persistent! results)))

(t/ann get-files [cred/GoogleCtx File -> (t/Vec File)])
(defn get-files
  "Returns a seq of files in the given folder"
  [google-ctx folder]
  (query-files google-ctx
               (str "'" (.getId folder) "' in parents and trashed=false")))

(t/ann folder? [File -> Boolean])
(defn folder?
  "Returns true if the file is a folder"
  [file]
  (= "application/vnd.google-apps.folder" (.getMimeType file)))

(t/ann folder-seq [cred/GoogleCtx File -> (t/Seq File)])
(defn folder-seq
  "Returns a lazy seq of all files in the given folder, including itself, via a
   depth-first traversal"
  [google-ctx folder]
  (tree-seq folder? (partial get-files google-ctx) folder))

(t/ann get-root-files [cred/GoogleCtx -> (t/Vec File)])
(defn get-root-files
  "Given a google-ctx configuration map, gets a seq of files from the user's
   root folder"
  [google-ctx]
  (query-files google-ctx "'root' in parents and trashed=false"))

(t/ann get-file [cred/GoogleCtx String -> File])
(defn get-file
  "Given a google-ctx configuration map and the id of the desired
  file as a string, returns that file as a drive File object"
  [google-ctx file-id]
  (let [drive-service (build-drive-service google-ctx)
        drive-files (doto (.files ^Drive drive-service)
                      assert)
        get-file (doto (.get drive-files file-id)
                   assert)]
    (cast File (doto (.execute get-file)
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

(t/ann delete-file [cred/GoogleCtx String -> File])
(defn delete-file
  "Given a google-ctx configuration map, and a file
   id to delete, moves that file to the trash"
  [google-ctx file-id]
  (let [drive-service (build-drive-service google-ctx)
        files (doto (.files ^Drive drive-service)
                assert)
        delete-request (doto (.trash files file-id)
                         assert)]
    (cast File (doto (.execute delete-request)
                 assert))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;; File Edits ;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

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

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;; File Properties Management ;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(t/ann get-properties [cred/GoogleCtx String -> (t/Seq Property)])
(defn get-properties
  "Given a google-ctx configuration map, and a file id, returns a
   list of all Properties associated with this file"
  [google-ctx file-id]
  (let [drive-service (build-drive-service google-ctx)
        properties (doto (.properties ^Drive drive-service)
                     assert)
        all-properties (doto (.list properties file-id)
                         assert)
        properties (cast PropertyList (doto (.execute all-properties)
                                        assert))]
    (tu/ignore-with-unchecked-cast
     (.getItems ^PropertyList properties)
     (t/Seq Property))))

(t/ann update-property [cred/GoogleCtx String String String String -> Property])
(defn update-property
  "Given a google-ctx configuration map, a file id, a key, a value, and
   a visibility(public or private) updates the property on this file to
   the new value if a property with the given key already exists, otherwise
   create a new one with this key value pair"
  [google-ctx file-id key value visibility]
  (let [drive-service (build-drive-service google-ctx)
        properties (doto (.properties ^Drive drive-service)
                     assert)
        property (doto (Property.)
                   (.setKey key)
                   (.setValue value)
                   (.setVisibility visibility))
        update-request (doto (.update properties file-id key property)
                         assert
                         (.setVisibility visibility))]
    (cast Property (doto (.execute update-request)
                     assert))))

(t/ann delete-property [cred/GoogleCtx String String String -> t/Any])
(defn delete-property
  "Given a google-ctx configuration map, a file id, and a key,
   deletes the property on this file associated with this key"
  [google-ctx file-id key visibility]
  (let [drive-service (build-drive-service google-ctx)
        properties (doto (.properties ^Drive drive-service)
                     assert)
        delete-request (doto (.delete properties file-id key)
                         assert
                         (.setVisibility visibility))]
    (.execute delete-request)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;; File Permissions Management ;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(t/ann get-permissions [cred/GoogleCtx String -> (t/Seq Permission)])
(defn get-permissions
  "Given a google-ctx configuration map, and a file-id, gets all of the
   permissions for the given file"
  [google-ctx file-id]
  (let [drive-service (build-drive-service google-ctx)
        permissions (doto (.permissions ^Drive drive-service)
                     assert)
        all-permissions (doto (.list permissions file-id)
                          assert)
        permissions (cast PermissionList (doto (.execute all-permissions)
                                           assert))]
    (tu/ignore-with-unchecked-cast
     (.getItems ^PermissionList permissions)
     (t/Seq Permission))))

(t/ann update-permission [cred/GoogleCtx String String String -> Permission])
(defn update-permission
  "Given a google-ctx configuration map, a file-id, an email address of the
   user who's permissions we are editing, and a new role for the user on this
   file(reader or writer, owner is not currently supported), adds or edits the
   permissions for this user on the given file"
  [google-ctx file-id email new-role]
  (let [drive-service (build-drive-service google-ctx)
        permissions (doto (.permissions ^Drive drive-service)
                      assert)
        permissions-for-file (tu/ignore-with-unchecked-cast
                              (set (map #(get % "emailAddress")
                                        (get-permissions google-ctx file-id)))
                              (t/Set String))
        id-request (doto (.getIdForEmail permissions email)
                        assert)
        permission-id (cast PermissionId (doto (.execute id-request)
                                              assert))
        permission-id (doto (.getId ^PermissionId permission-id)
                        assert)
        permission (doto (Permission.)
                     (.setEmailAddress email)
                     (.setRole new-role)
                     (.setId permission-id)
                     (.setType "user"))
        request (if (contains? permissions-for-file email)
                  (doto (.update permissions file-id permission-id permission)
                    assert)
                  (doto (.insert permissions file-id permission)
                    assert))]
    (tu/ignore-with-unchecked-cast (.execute request)
                                   Permission)))

(t/ann remove-permission [cred/GoogleCtx String String -> t/Any])
(defn remove-permission
  "Given a google-ctx configuration map, a file-id, and  an email address
   of the user who's permissions we are editing, removes this user from
   the permissions of the given file"
  [google-ctx file-id email]
  (let [drive-service (build-drive-service google-ctx)
        permissions (doto (.permissions ^Drive drive-service)
                      assert)
        permissions-for-file (tu/ignore-with-unchecked-cast
                              (set (map #(get % "emailAddress")
                                        (get-permissions google-ctx file-id)))
                              (t/Set String))
        id-request (doto (.getIdForEmail permissions email)
                        assert)
        permission-id (cast PermissionId (doto (.execute id-request)
                                              assert))
        permission-id (doto (.getId ^PermissionId permission-id)
                        assert)
        delete-request (doto (.delete permissions file-id permission-id)
                         assert)]
    (if (contains? permissions-for-file email)
      (.execute delete-request))))
