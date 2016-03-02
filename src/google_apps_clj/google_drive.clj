(ns google-apps-clj.google-drive
  "A library for connecting to Google Drive through the Drive API"
  (:require [clojure.core.typed :as t]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [google-apps-clj.credentials :as cred])
  (:import (com.google.api.client.googleapis.batch BatchRequest
                                                   BatchCallback)
           (com.google.api.client.googleapis.json GoogleJsonError
                                                  GoogleJsonError$ErrorInfo
                                                  GoogleJsonErrorContainer
                                                  GoogleJsonResponseException)
           (com.google.api.client.http FileContent
                                       InputStreamContent
                                       GenericUrl)
           (com.google.api.client.util GenericData)
           (com.google.api.services.drive Drive
                                          Drive$Builder
                                          Drive$Files$Delete
                                          Drive$Files$Get
                                          Drive$Files$Insert
                                          Drive$Files$List
                                          Drive$Files$Update
                                          Drive$Permissions$Delete
                                          Drive$Permissions$Insert
                                          Drive$Permissions$List
                                          Drive$Permissions$Update
                                          DriveRequest
                                          DriveScopes)
           (com.google.api.services.drive.model File
                                                File$Labels
                                                FileList
                                                ParentReference
                                                Permission
                                                PermissionId
                                                PermissionList
                                                Property
                                                PropertyList
                                                User)
           (java.io InputStream)
           (com.google.api.client.googleapis.auth.oauth2 GoogleCredential)))

;;; TODO this does not type check now due to protocol (ab)use and general
;;; unfamiliarity with types when I first rewrote it


;; General type annotations for external things

(t/ann ^:no-check clojure.core/slurp [java.io.InputStream -> String])
(t/ann ^:no-check clojure.java.io/input-stream [t/Any -> java.io.InputStream])

(t/non-nil-return com.google.api.services.drive.Drive/files :all)
(t/non-nil-return com.google.api.services.drive.Drive$Files/delete :all)
(t/non-nil-return com.google.api.services.drive.Drive$Files/get :all)
(t/non-nil-return com.google.api.services.drive.Drive$Files/insert :all)
(t/non-nil-return com.google.api.services.drive.Drive$Files/list :all)
(t/non-nil-return com.google.api.services.drive.Drive$Files/update :all)

(t/non-nil-return com.google.api.services.drive.Drive/permissions :all)
(t/non-nil-return com.google.api.services.drive.Drive$Permissions/delete :all)
(t/non-nil-return com.google.api.services.drive.Drive$Permissions/insert :all)
(t/non-nil-return com.google.api.services.drive.Drive$Permissions/list :all)
(t/non-nil-return com.google.api.services.drive.Drive$Permissions/update :all)

;; Basic helper methods
;; TODO: consider moving to a `util` namespace if it's not google-drive specific?

(t/ann bool? [t/Any -> t/Bool])
(defn bool? [v]
  (or (true? v) (false? v)))


;; Basic shared data types

(t/defalias FileId t/Str)
(t/defalias FolderId t/Str)
(t/defalias FieldList (t/Seq (t/U t/Keyword t/Str)))
(t/defalias FileUploadContent t/Any)
(t/defalias PermissionIdentifier t/Str)
(t/defalias Role (t/U ':owner ':writer ':reader))
(t/defalias PermissionType (t/U ':user ':group ':domain ':anyone))


(t/defalias FileDeleteQuery
  (t/HMap
    :mandatory {:model   ':files
                :action  ':delete
                :file-id FileId}
    :complete? true))

(t/defalias FileListQuery
  (t/HMap
    :mandatory {:model  ':files
                :action ':list}
    :optional {:query  t/Str
               :fields FieldList}
    :complete? true))

(t/defalias FileGetQuery
  (t/HMap
    :mandatory {:model   ':files
                :action  ':get
                :file-id FileId}
    :optional {:fields FieldList}
    :complete? true))

(t/defalias FileInsertQuery
  (t/HMap
    :mandatory {:model  ':files
                :action ':insert
                :title  t/Str}
    :optional {:fields             FieldList
               :description        t/Str
               :parent-ids         (t/Seq FolderId)
               :writers-can-share? t/Bool
               :direct-upload?     t/Bool
               :convert?           t/Bool
               :mime-type          (t/Option t/Str)
               :content            FileUploadContent
               ;TODO non-negative integer for content-length
               :content-length     t/Int}
    :complete? true))

(t/defalias FileUpdateQuery
  (t/HMap
    :mandatory {:model   ':files
                :action  ':update
                :file-id FileId}
    :optional {:fields             FieldList
               :description        t/Str
               :parent-ids         (t/Seq FolderId)
               :writers-can-share? t/Bool
               :direct-upload?     t/Bool
               :convert?           t/Bool
               :mime-type          (t/Option t/Str)
               :content            FileUploadContent
               ;TODO non-negative integer for content-length
               :content-length     t/Int}
    :complete? true))

(t/defalias PermissionDeleteQuery
  (t/HMap
    :mandatory {:model         ':permissions
                :action        ':delete
                :file-id       FileId
                :permission-id PermissionIdentifier}
    :complete? true))

(t/defalias PermissionInsertQuery
  (t/HMap
    :mandatory {:model   ':permissions
                :action  ':insert
                :file-id FileId
                :role    Role
                :type    PermissionType
                :value   t/Str}
    :optional {:with-link? t/Bool
               :fields     FieldList}
    :complete? true))

(t/defalias PermissionListQuery
  (t/HMap
    :mandatory {:model   ':permissions
                :action  ':list
                :file-id FileId}
    :optional {:fields FieldList}
    :complete? true))

(t/defalias PermissionUpdateQuery
  (t/HMap
    :mandatory {:model         ':permissions
                :action        ':update
                :file-id       FileId
                :permission-id PermissionIdentifier
                :role          Role}
    :optional {:fields              FieldList
               :with-link?          t/Bool
               :transfer-ownership? t/Bool}
    :complete? true))

(t/defalias Query
  (t/U FileDeleteQuery
       FileGetQuery
       FileInsertQuery
       FileListQuery
       FileUpdateQuery
       PermissionDeleteQuery
       PermissionInsertQuery
       PermissionListQuery
       PermissionUpdateQuery))

(t/defalias Request
  (t/U Drive$Files$Delete
       Drive$Files$Get
       Drive$Files$Insert
       Drive$Files$List
       Drive$Files$Update
       Drive$Permissions$Delete
       Drive$Permissions$Insert
       Drive$Permissions$List
       Drive$Permissions$Update))


;; Helper methods

(t/ann build-file [(t/U FileInsertQuery FileUpdateQuery) -> File])
(defn- ^File build-file
  [query]
  (let [{:keys [description mime-type parent-ids title writers-can-share?]} query
        parents (when (seq parent-ids)
                  (map (t/fn [id :- FolderId]
                         (doto (ParentReference.)
                           (.setId id)))
                       parent-ids))
        file (new File)]
    (when description (.setDescription file description))
    (when mime-type (.setMimeType file mime-type))
    (when parents (.setParents file parents))
    (when title (.setTitle file title))
    (when (bool? writers-can-share?) (.setWritersCanShare file (boolean writers-can-share?)))
    ;result is a file returned to the user
    file))


(t/ann build-stream [(t/U FileInsertQuery FileUpdateQuery) -> (t/Option InputStreamContent)])
(defn- build-stream
  [query]
  (when-let [content (:content query)]
    (let [mime-type (:mime-type query)
          content-length (:content-length query)
          input-stream (io/input-stream content)
          content-stream (new InputStreamContent (or mime-type "") input-stream)]
      (when (integer? content-length) (.setLength content-stream (long content-length)))
      content-stream)))


;TODO: should this logic get pushed out into the individual `*->DriveRequest` methods?
(t/ann format-fields-string [Query -> (t/Option t/Str)])
(defn- format-fields-string
  [qmap]
  (let [{:keys [model action fields]} qmap
        ; TODO more rigorous support for nesting, e.g. permissions(role,type)
        fields (when (seq fields) (string/join "," (map name fields)))
        items? (= :list action)
        fields-seq (cond-> []
                           (and items? (= model :files)) (conj "nextPageToken")
                           (and items? fields) (conj (format "items(%s)" fields))
                           (and items? (not fields)) (conj "items")
                           (and (not items?) fields) (conj fields))
        fields (when (seq fields-seq) (string/join "," fields-seq))]
    fields))


;TODO: remove me
(defmacro cond-doto
  [x & forms]
  (assert (even? (count forms)))
  (let [gx (gensym)]
    `(let [~gx ~x]
       ~@(map (fn [[test expr]]
                (if (seq? expr)
                  `(when ~test (~(first expr) ~gx ~@(next expr)))
                  `(when ~test (~expr ~gx))))
              (partition 2 forms))
       ~gx)))



;; Deleting a single file

(t/ann file-delete-query [FileId -> FileDeleteQuery])
(defn file-delete-query
  [file-id]
  {:model   :files
   :action  :delete
   :file-id file-id})

(t/ann FileDeleteQuery->DriveRequest [Drive FileDeleteQuery -> Drive$Files$Delete])
(defn FileDeleteQuery->DriveRequest
  [^Drive drive-service, qmap]
  (let [files-service (.files drive-service)]
    (.delete files-service (:file-id qmap))))


;; Listing files

(t/ann file-list-query (t/IFn [-> FileListQuery] [(t/HMap) -> FileListQuery]))
(defn file-list-query
  ([] (file-list-query []))
  ([extras]
   (merge extras {:model :files, :action :list})))

(t/ann folder-list-files-query (t/IFn [FolderId -> FileListQuery] [FolderId (t/HMap) -> FileListQuery]))
(defn folder-list-files-query
  ([folder-id] (folder-list-files-query folder-id {}))
  ([folder-id extras]
   (file-list-query (merge extras {:query (format "'%s' in parents" folder-id)}))))


(t/ann all-files-query (t/IFn [-> FileListQuery] [(t/HMap) -> FileListQuery]))
(defn all-files-query
  ([] (all-files-query {}))
  ([extras]
   (let [fields [:id :title :writersCanShare :mimeType
                 "permissions(emailAddress,type,domain,role,withLink)"
                 "owners(emailAddress)"
                 "parents(id)"]]
     (file-list-query (merge {:fields fields :query "trashed=false"} extras)))))

(t/ann FileListQuery->DriveRequest [Drive FileListQuery -> Drive$Files$List])
(defn FileListQuery->DriveRequest
  [^Drive drive-service, qmap]
  (let [files-service (.files drive-service)
        request (.list files-service)]
    (when-let [query (:query qmap)] (.setQ request query))
    (when-let [fields (format-fields-string qmap)] (.setFields request fields))
    request))


;; Grabbing a single file

(t/ann file-get-query [FileId -> FileGetQuery])
(defn file-get-query
  [file-id]
  {:model   :files
   :action  :get
   :file-id file-id})

(t/ann FileGetQuery->DriveRequest [Drive FileGetQuery -> Drive$Files$Get])
(defn FileGetQuery->DriveRequest
  [^Drive drive-service, qmap]
  (let [files-service (.files drive-service)
        request (.get files-service (:file-id qmap))]
    (when-let [fields (format-fields-string qmap)] (.setFields request fields))
    request))


;; Inserting (uploading) a single file or folder

(t/ann file-insert-query [FolderId FileUploadContent t/Str (t/HMap) -> FileInsertQuery])
(defn file-insert-query
  [folder-id content file-title {:keys [mime-type convert?] :as extra-args}]
  (merge extra-args
         {:model      :files
          :action     :insert
          :parent-ids [folder-id]
          :title      file-title
          :convert?   (if (bool? convert?) convert? (some? mime-type))
          :content    content}))

(t/ann FileInsertQuery->DriveRequest [Drive FileInsertQuery -> Drive$Files$Insert])
(defn FileInsertQuery->DriveRequest
  [^Drive drive-service, qmap]
  (let [file (build-file qmap)
        stream (build-stream qmap)
        convert? (boolean (get-in qmap [:convert?] true))
        files-service (.files drive-service)
        request (if stream
                  (doto (.insert files-service file stream)
                    (.setConvert convert?))
                  (.insert files-service file))]
    ;Allow direct upload, which is more efficient for tiny files
    ;https://developers.google.com/api-client-library/java/google-api-java-client/media-upload#direct
    (when (:direct-upload? qmap)
      (when-let [uploader (.getMediaHttpUploader request)]
        (.setDirectUploadEnabled uploader true)))
    (when-let [fields (format-fields-string qmap)] (.setFields request fields))
    request))


;; Helpers for dealing with folders

;Inserted folders have a special mime-type
;see https://developers.google.com/drive/v3/web/folder
(t/ann folder-mime-type t/Str)
(def folder-mime-type "application/vnd.google-apps.folder")

(t/ann folder? [(t/HMap :mandatory {:mime-type t/Str}) -> t/Bool])
(defn folder?
  "Predicate fn indicating if the given file map has the folder mime type"
  [file-map]
  (= folder-mime-type (:mime-type file-map)))

(t/ann folder-insert-query [FolderId t/Str -> FileInsertQuery])
(defn folder-insert-query
  [parent-id title]
  {:model      :files
   :action     :insert
   :parent-ids [parent-id]
   :mime-type  folder-mime-type
   :title      title})


;; Updating a single file (either moving it, or changing contents, or potentially both)

(t/ann file-update-query [FileId (t/HMap) -> FileUpdateQuery])
(defn file-update-query
  [file-id extra-args]
  (merge extra-args
         {:model    :files
          :action   :update
          :file-id  file-id
          :convert? (boolean (:convert? extra-args))}))

(t/ann FileUpdateQuery->DriveRequest [Drive FileUpdateQuery -> Drive$Files$Update])
(defn FileUpdateQuery->DriveRequest
  [^Drive drive-service, qmap]
  (let [file (build-file qmap)
        file-id (:file-id qmap)
        stream (build-stream qmap)
        convert? (boolean (get-in qmap [:convert?] false))
        files-service (.files drive-service)
        request (if stream
                  (doto (.update files-service file-id file stream)
                    (.setConvert convert?))
                  (.update files-service file-id file))]
    ;Allow direct upload, which is more efficient for tiny files
    ;https://developers.google.com/api-client-library/java/google-api-java-client/media-upload#direct
    (when (:direct-upload? qmap)
      (when-let [uploader (.getMediaHttpUploader request)]
        (.setDirectUploadEnabled uploader true)))
    (when-let [fields (format-fields-string qmap)] (.setFields request fields))
    request))


(t/ann file-move-query [FolderId FileId -> FileUpdateQuery])
(defn file-move-query
  [folder-id file-id]
  (file-update-query file-id {:parent-ids [folder-id]}))

;; Enumerating permissions on a single entity







(t/ann build-drive-service [cred/GoogleAuth -> Drive])
(defn ^Drive build-drive-service
  "Given a google-ctx configuration map, builds a Drive service using
   credentials coming from the OAuth2.0 credential setup inside google-ctx"
  [google-ctx]
  (let [drive-builder (->> google-ctx
                           cred/build-credential
                           (Drive$Builder. cred/http-transport cred/json-factory))]
    ;Sets the application name to be used in the UserAgent header of each request, or nil for none
    (.setApplicationName drive-builder "google-apps-clj")
    (cast Drive (doto (.build drive-builder)
                  assert))))




(t/ann root-id t/Str)
(def root-id
  "The id of the root folder"
  "root")


(t/ann build-request [cred/GoogleAuth Query -> Request])
(defn- ^DriveRequest build-request
  "Converts a query into a stateful request object executable in the
   given google context. Queries are maps with the following required
   fields:

   :model - :files, :permissions
   :action - :list, :get, :update, :insert, :delete

   Other fields may be given, and may be required by the action and model.
   These may include:

   :fields - a seq of keywords specifying the object projection
   :query - used to constrain a list of files
   :file-id - specifies the file for file-specific models and actions"
  [google-ctx query]
  (let [drive (build-drive-service google-ctx)
        {:keys [model action]} query
        fields (format-fields-string query)]
    (case model
      :files
      (case action
        :delete (FileDeleteQuery->DriveRequest drive query)
        :list   (FileListQuery->DriveRequest drive query)
        :get    (FileGetQuery->DriveRequest drive query)
        :update (FileUpdateQuery->DriveRequest drive query)
        :insert (FileInsertQuery->DriveRequest drive query))
      :permissions
      (case action
        :list
        (let [{:keys [file-id]} query
              request (.list (.permissions drive) file-id)]
          (cond-doto ^DriveRequest request
            fields (.setFields fields)))
        :insert
        (let [{:keys [file-id value role type with-link?]} query
              permission (doto (Permission.)
                             (.setRole (name role))
                             (.setType (name type))
                             (.setValue value)
                             (cond-doto (not (nil? with-link?))
                               (.setWithLink with-link?)))
              request (doto (.insert (.permissions drive) file-id permission)
                        (.setSendNotificationEmails false))]
          (cond-doto ^DriveRequest request
            fields (.setFields fields)))
        :update
        (let [{:keys [file-id permission-id role transfer-ownership?]} query
              permission (doto (Permission.)
                           (.setRole (name role)))
              request (cond-doto (.update (.permissions drive)
                                          file-id permission-id permission)
                        (not (nil? transfer-ownership?))
                        (.setTransferOwnership transfer-ownership?))]
          (cond-doto ^DriveRequest request
            fields (.setFields fields)))
        :delete
        (let [{:keys [file-id permission-id]} query]
          (.delete (.permissions drive) file-id permission-id))))))

(defprotocol Requestable
  (response-data
   [request response]
   "Extracts the good bit from the response")
  (next-page!
   [request response]
   "Mutates the request to retrieve the next page of results if supported and
    present"))

(extend-protocol Requestable
  Drive$Files$Delete
  (next-page! [request response])
  (response-data [request response]
    response)

  Drive$Files$Get
  (next-page! [request response])
  (response-data [request response]
    response)

  Drive$Files$Insert
  (next-page! [request response])
  (response-data [request response]
    response)

  Drive$Files$List
  (next-page! [request response]
    (when response
      (when-let [page-token (.getNextPageToken ^FileList response)]
        (.setPageToken request page-token))))
  (response-data [request response]
    (when response
      (.getItems ^FileList response)))

  Drive$Files$Update
  (next-page! [request response])
  (response-data [request response]
    response)

  Drive$Permissions$Delete
  (next-page! [request response])
  (response-data [request response]
    response)

  Drive$Permissions$Insert
  (next-page! [request response])
  (response-data [request response]
    response)

  Drive$Permissions$List
  (next-page! [request response])
  (response-data [request response]
    (when response
      (.getItems ^PermissionList response)))

  Drive$Permissions$Update
  (next-page! [request response])
  (response-data [request response]
    response))

;; TODO perhaps validate the codomain is a subset of the keyword domain
(t/ann camel->kebab [t/Str -> t/Str])
(defn- camel->kebab
  [camel]
  (let [accum (StringBuffer.)]
    (loop [[^Character c & cs] camel]
      (if (not c)
        (.toString accum)
        (let [c' (Character/toLowerCase c)]
          (when-not (= c' c)
            (.append accum \-))
          (.append accum c')
          (recur cs))))
    (.toString accum)))

(t/ann convert-bean [java.util.Map -> (t/HMap)])
(defn- convert-bean
  [bean]
  (->> (keys bean)
       (map (juxt (comp keyword camel->kebab) (partial get bean)))
       (into {})))

(defprotocol Response
  (convert-response
   [_]
   "Convert the google response object into a clojure form"))

(extend-protocol Response
  java.util.List
  (convert-response [l]
    (mapv convert-response l))
  com.google.api.client.util.ArrayMap
  (convert-response [m]
    m)
  com.google.api.client.json.GenericJson
  (convert-response [m]
    (->> (keys m)
         (map (fn [field]
                (let [value (convert-response (get m field))
                      field (if-not (true? value)
                              (-> field camel->kebab keyword)
                              (-> field camel->kebab (str "?") keyword))]
                  [field value])))
         (into {})))
  com.google.api.client.util.DateTime
  (convert-response [dt]
    ;; TODO convert to inst or jodatime
    dt)
  java.lang.String
  (convert-response [s]
    s)
  java.lang.Long
  (convert-response [l]
    l)
  java.lang.Boolean
  (convert-response [b]
    b)
  nil
  (convert-response [_]))

(t/ann rate-limit-exceeded? [GoogleJsonError -> t/Bool])
(defn- rate-limit-exceeded?
  [^GoogleJsonError error]
  (and (= 403 (.getCode error))
       (some (fn [^GoogleJsonError$ErrorInfo error]
               (let [reason (.getReason error)]
                 (case reason
                   "rateLimitExceeded" true
                   "userRateLimitExceeded" true
                   false)))
             (.getErrors error))))

(defn execute-query!
  "Executes the given query in the google context and returns the
   results converted into clojure forms. If the response is paginated,
   all results are fetched and concatenated into a vector."
  [google-ctx query]
  (let [request (build-request google-ctx query)
        results (atom nil)]
    (loop []
      (let [response (.execute request)
            data (convert-response (response-data request response))]
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
  "Execute the given queries in a batch, returning their responses,
   converted into clojure forms, in the same order as the queries. If
   any queries in a batch yield paginated responses, another batch will
   be executed for all such queries, iteratively until all pages have
   been received, and the results concatenated into vectors as in
   execute!.

   Queries that yield error results due to rate limits are retried
   after sleeping up to 200ms. This sleep is cumulative for the batch.
   There is no limit on the number of rate limit retries. All other
   errors are given as GoogleJsonError objects in the responses."
  [google-ctx queries]
  ;; TODO partition queries into batches?
  (let [requests (map (partial build-request google-ctx) queries)
        credential (cred/build-credential google-ctx)
        batch (BatchRequest. cred/http-transport credential)
        responses (atom (into [] (repeat (count requests) nil)))]
    (loop [requests (map-indexed vector requests)]
      (let [next-requests (atom {})]
        (doseq [[i ^DriveRequest request] requests]
          (.queue request batch GoogleJsonErrorContainer
                  (reify BatchCallback
                    (onSuccess [_ response headers]
                      (let [data (convert-response (response-data request response))]
                        (if (next-page! request response)
                          (do
                            (swap! next-requests assoc i request)
                            (swap! responses
                                   (fn [responses]
                                     (let [extant (nth responses i)
                                           response (into (or extant []) data)]
                                       (assoc responses i response)))))
                          (swap! responses
                                 (fn [responses]
                                   (let [extant (nth responses i)
                                         response (if extant
                                                    (into extant data)
                                                    data)]
                                     (assoc responses i response)))))))
                    (onFailure [_ container headers]
                      (let [error (.getError ^GoogleJsonErrorContainer container)]
                        (if (rate-limit-exceeded? error)
                          (do
                            (Thread/sleep (+ 100 (rand-int 100)))
                            (swap! next-requests assoc i request))
                          (swap! responses assoc i error)))))))
        (.execute batch)
        (let [next-requests @next-requests]
          (when (seq next-requests)
            (recur next-requests)))))
    @responses))

(def batch-size
  "Google internally unwraps batches and processes them concurrently. Batches
   that are too large can cause the request to exceed Google's api rate limit,
   which is applied to api requests, not http requests."
  20)

(defn execute!
  "Executes the given queries in the most efficient way, returning their
   results in a seq of clojure forms. Note the queries may be processed
   concurrently."
  [google-ctx queries]
  (when (seq queries)
    (if (= 1 (count queries))
      [(execute-query! google-ctx (first queries))]
      (let [batches (partition-all batch-size queries)]
        (doall (mapcat (partial execute-batch! google-ctx) batches))))))

;;;; Commands and their helpers

(t/ann derive-type [t/Str -> PermissionType])
(defn- derive-type
  [^String principal]
  (cond (= "anyone" principal)
        :anyone
        (pos? (.indexOf principal "@"))
        :user ; This seems to work correctly for users and groups
        :else
        :domain))

(defn- derive-principal
  [permission]
  (let [{:keys [type email-address domain]} permission]
    (case type
      "anyone" "anyone"
      "domain" domain
      "group" email-address
      "user" email-address)))

(defn get-permissions!
  "Returns the permissions granted on the given file, filtered for those
   explicitly granted to the principal if given"
  ([google-ctx file-id]
   (get-permissions! google-ctx file-id false))
  ([google-ctx file-id principal]
   (let [list-query {:model :permissions
                     :action :list
                     :file-id file-id
                     :fields [:id :role :withLink :type :domain :emailAddress]}
         permissions (execute-query! google-ctx list-query)]
     (cond->> permissions
       principal (filter (fn [permission]
                           (condp = (derive-type principal)
                             :user
                             (and (= principal (:email-address permission))
                                  (#{"user" "group"} (:type permission)))
                             :domain
                             (and (= principal (:domain permission))
                                  (= "domain" (:type permission)))
                             :anyone
                             (and (= "anyone" (:type permission))))))))))

(defn summarize-permissions
  "Returns a map of the sets of principals in the given permissions grouped by
   role"
  [permissions]
  (reduce (fn [accum permission]
            (let [{:keys [role]} permission]
              (update-in accum [role] (fnil conj #{})
                         (derive-principal permission))))
          {}
          permissions))

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
  [google-ctx file-id authorization]
  (let [{:keys [principal role searchable?]} authorization
        extant (get-permissions! google-ctx file-id principal)
        principal-id (atom nil) ; TODO this could be a volatile
        ids-to-delete (atom [])]
    ;; [principal withLink] seem to be a unique key within a file
    (doseq [permission extant]
      (if (and (= (name role) (:role permission))
               (case searchable?
                 true (true? (:with-link? permission))
                 false (nil? (:with-link? permission))))
        (reset! principal-id (:id permission))
        (swap! ids-to-delete conj (:id permission))))
    (let [deletes (map (fn [id] {:model :permissions
                                 :action :delete
                                 :file-id file-id
                                 :permission-id id})
                       @ids-to-delete)
          insert (when-not @principal-id
                   {:model :permissions
                    :action :insert
                    :file-id file-id
                    :value principal
                    :role role
                    :type (derive-type principal)
                    :with-link? searchable?
                    :fields [:id]})]
      (execute! google-ctx deletes)
      (when insert
        (execute-query! google-ctx insert)))
    nil))

(defn revoke!
  "Revoke all authorizations for the given principal on the given file. The
   principal may be the literal \"anyone\", an email address of a user or
   google app group, or a google app domain.

   This operation should be idempotent until the the permissions change by some
   other operation."
  [google-ctx file-id principal]
  (let [extant (get-permissions! google-ctx file-id principal)
        deletes (mapv (fn [permission]
                        {:model :permissions
                         :action :delete
                         :file-id file-id
                         :permission-id (:id permission)})
                      extant)]
    (execute! google-ctx deletes)
    nil))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn create-folder!
  "Create a folder with the given title in the given parent folder"
  [google-ctx parent-id title]
  (execute-query! google-ctx (folder-insert-query parent-id title)))


(defn move-file!
  "Moves a file to a folder. This returns true if successful, false
   if forbidden, and raises otherwise."
  [google-ctx folder-id file-id]
  (try
    (execute-query! google-ctx (file-move-query folder-id file-id))
    true
    (catch GoogleJsonResponseException e
      (when (not= 400 (.getStatusCode e))
        (throw e))
      false)))


(t/ann upload-file! (t/IFn [cred/GoogleAuth t/Str t/Any t/Str -> t/Any]
                           [cred/GoogleAuth t/Str t/Any t/Str (t/HMap) -> t/Any]))
(defn upload-file!
  "Uploads a file with the given title and content into the specified folder.
  Additional upload options can be specified in a map"
  ([google-auth folder-id content file-title]
   (upload-file! google-auth folder-id content file-title {}))
  ([google-auth folder-id content file-title extra-args]
   (let [query-map (file-insert-query folder-id content file-title extra-args)]
     (execute-query! google-auth query-map))))

;TODO: annotate
(defn download-file!
  "Downloads the contents of the given file as an inputstream, or nil if the
   file is not available or is not available in the given mime type."
  ([google-ctx file]
   (download-file! google-ctx file nil))
  ([google-ctx file mime-type]
   (when (:id file)
     (let [drive (build-drive-service google-ctx)]
       (if mime-type
         (when-let [url (get (:export-links file) mime-type)]
           ;; This is purely to force authentication on the stupid drive request
           ;; factory. There is almost certainly a better way to handle this,
           ;; either locally or by reconsidering the google-ctx object
           (.execute (.setFields (.get (.files drive) (:id file)) "id"))
           (let [http-request (.getRequestFactory drive)
                 gurl (GenericUrl. ^String url)
                 _ (prn "gurl" gurl)
                 get-request (.buildGetRequest http-request gurl)
                 response (.execute get-request)]
             (.getContent response)))
         (when-let [url (:download-url file)]
           (let [drive (build-drive-service google-ctx)]
             (.executeMediaAsInputStream (.get (.files drive) ^String (:id file))))))))))


;TODO: move me up next to query construction?
;TODO: annotate
(defn delete-file!
  "Permanently deletes the given file. If the file is a folder, this also
   deletes all of its descendents."
  [google-ctx file-id]
  (execute-query! google-ctx (file-delete-query file-id)))

;TODO: move me up next to query construction?
;TODO: annotate
(defn list-files!
  "Returns a seq of files in the given folder"
  ([google-ctx folder-id]
   (list-files! google-ctx folder-id {}))
  ([google-ctx folder-id extras]
   (execute-query! google-ctx (folder-list-files-query folder-id extras))))

;TODO: move me up next to query construction?
;TODO: annotate
(defn get-file!
  "Returns the metadata for the given file"
  [google-ctx file-id]
  (execute-query! google-ctx (file-get-query file-id)))

;TODO: clean up and maybe pull into separate parts?
(defn find-file!
  "Given a path as a seq of titles relative to the given folder id,
   returns the file if there is one. If any title matches more than one
   file, this raises an error. If the fields are specified, they
   specify the attributes requested of the ultimate file, otherwise the
   defaults are used."
  ([google-ctx parent-id path]
   (find-file! google-ctx parent-id path nil))
  ([google-ctx parent-id path fields]
   (when (seq path)
     (loop [folder-id parent-id
            [title & path'] path]
       (let [q (format "'%s' in parents and title = '%s' and trashed=false"
                       folder-id title)
             fields (if (seq path')
                      [:id]
                      fields)
             query (cond-> {:model :files
                            :action :list
                            :query q}
                     (seq fields)
                     (assoc :fields fields))
             results (execute-query! google-ctx query)
             total (count results)]
         (when (seq results)
           (when (seq (rest results))
             (let [msg (format "Can't resolve path %s, too many matches for %s"
                               (pr-str path) title)]
               (throw (IllegalStateException. msg))))
           (let [file (first results)]
             (if (seq path')
               (recur (:id file) path')
               file))))))))
