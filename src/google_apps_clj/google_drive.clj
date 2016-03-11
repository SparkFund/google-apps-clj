(ns google-apps-clj.google-drive
  "A library for connecting to Google Drive through the Drive API"
  (:require
    [clojure.core.typed :as t]
    [clojure.java.io :as io]
    [clojure.string :as string]
    [google-apps-clj.credentials :as cred]
    [google-apps-clj.google-drive.mime-types :as gdrive-mime]
    [google-apps-clj.google-drive.constants :as gdrive-const])
  (:import
    (com.google.api.client.googleapis.batch
      BatchRequest
      BatchCallback)
    (com.google.api.client.googleapis.json
      GoogleJsonError
      GoogleJsonError$ErrorInfo
      GoogleJsonErrorContainer
      GoogleJsonResponseException)
    (com.google.api.client.http
      InputStreamContent
      GenericUrl)
    (com.google.api.services.drive
      Drive
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
      DriveRequest)
    (com.google.api.services.drive.model
      File
      FileList
      ParentReference
      Permission
      PermissionList)))

;;General type annotations for external things
;;Needed to keep the type checker happy

(t/ann ^:no-check clojure.core/slurp [java.io.InputStream -> String])
(t/ann ^:no-check clojure.java.io/input-stream [t/Any -> java.io.InputStream])
(t/ann ^:no-check com.google.api.client.googleapis.json.GoogleJsonError/getErrors [-> (java.util.List GoogleJsonError$ErrorInfo)])

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
(t/defalias FieldList (t/Seqable (t/U t/Keyword t/Str)))
(t/defalias FileUploadContent t/Any)
(t/defalias PermissionId t/Str)
(t/defalias Role (t/U ':owner ':writer ':reader))
(t/defalias PermissionType (t/U nil ':user ':group ':domain ':anyone))


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
               :parent-ids         (t/Seqable FolderId)
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
               :parent-ids         (t/Seqable FolderId)
               :writers-can-share? t/Bool
               :direct-upload?     t/Bool
               :convert?           t/Bool
               :mime-type          (t/Option t/Str)
               :content            FileUploadContent
               ;TODO non-negative integer for content-length
               :content-length     t/Int}
    :complete? true))

(t/defalias PermissionListQuery
  (t/HMap
    :mandatory {:model   ':permissions
                :action  ':list
                :file-id FileId}
    :optional {:fields FieldList}
    :complete? true))

(t/defalias PermissionDeleteQuery
  (t/HMap
    :mandatory {:model         ':permissions
                :action        ':delete
                :file-id       FileId
                :permission-id PermissionId}
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

(t/defalias PermissionUpdateQuery
  (t/HMap
    :mandatory {:model         ':permissions
                :action        ':update
                :file-id       FileId
                :permission-id PermissionId
                :role          Role}
    :optional {:fields              FieldList
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
  (t/U Drive$Files$List
       Drive$Files$Get
       Drive$Files$Insert
       Drive$Files$Update
       Drive$Files$Delete
       Drive$Permissions$List
       Drive$Permissions$Insert
       Drive$Permissions$Update
       Drive$Permissions$Delete))

;Return type when invoking `.execute` on one of the `Request` types, above
(t/defalias ResponseRaw
  (t/U nil
       File
       Permission
       FileList
       PermissionList))

;A processed response
(t/defalias ResponseData
  (t/U nil
       File
       Permission
       (java.util.List File)
       (java.util.List Permission)))

(t/defalias QueryResult
  (t/U (t/Seqable QueryResult)
       (t/HMap)
       java.lang.Number
       java.lang.String
       java.lang.Boolean
       clojure.lang.Keyword
       java.util.Date
       com.google.gdata.data.DateTime
       com.google.api.client.util.DateTime
       nil))

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
(defn- ^String format-fields-string
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

(t/ann guess-principal-type [(t/Option t/Str) -> PermissionType])
(defn- guess-principal-type
  [^String principal]
  (cond
    (nil? principal) nil
    (= "anyone" principal) :anyone
    ;This seems to work correctly for users and groups
    (> (.indexOf principal "@") 0) :user
    :else :domain))

(defn- derive-principal
  [permission]
  (let [{:keys [type email-address domain]} permission]
    (case type
      "anyone" "anyone"
      "domain" domain
      "group" email-address
      "user" email-address)))


(t/ann permission-has-principal? [(t/Option t/Str) t/Any -> t/Bool])
(defn permission-has-principal?
  [^String principal, permission]
  (let [perm-type (:type permission)]
    (case (guess-principal-type principal)
      :user (and (= principal (:email-address permission))
                 (or (= "user" perm-type) (= "group" perm-type)))
      :domain (and (= principal (:domain permission))
                   (= "domain" perm-type))
      :anyone (and (= "anyone" perm-type))
      false)))


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

(t/ann ^:no-check file-list-query (t/IFn [-> FileListQuery] [(t/HMap) -> FileListQuery]))
(defn file-list-query
  ([] (file-list-query {}))
  ([extras]
   (merge extras {:model :files, :action :list})))

(t/ann ^:no-check folder-list-files-query (t/IFn [FolderId -> FileListQuery] [FolderId (t/HMap) -> FileListQuery]))
(defn folder-list-files-query
  ([folder-id] (folder-list-files-query folder-id {}))
  ([folder-id extras]
   (file-list-query (merge extras {:query (format "'%s' in parents" folder-id)}))))


(t/ann ^:no-check all-files-query (t/IFn [-> FileListQuery] [(t/HMap) -> FileListQuery]))
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

(t/ann ^:no-check file-insert-query [FolderId FileUploadContent t/Str (t/HMap) -> FileInsertQuery])
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
;; See also https://developers.google.com/drive/v3/web/folder

(t/ann folder? [(t/HMap :mandatory {:mime-type t/Str}) -> t/Bool])
(defn folder?
  "Predicate fn indicating if the given file map has the folder mime type"
  [file-map]
  (= gdrive-mime/folder (:mime-type file-map)))

(t/ann folder-insert-query [FolderId t/Str -> FileInsertQuery])
(defn folder-insert-query
  [parent-id title]
  {:model      :files
   :action     :insert
   :parent-ids [parent-id]
   :mime-type  gdrive-mime/folder
   :title      title})


;; Updating a single file (either moving it, or changing contents, or potentially both)

(t/ann ^:no-check file-update-query [FileId (t/HMap) -> FileUpdateQuery])
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

;; Enumerating permissions on a file/folder

(t/ann ^:no-check permission-list-query (t/IFn [FileId -> PermissionListQuery]
                                               [FileId (t/HMap) -> PermissionListQuery]))
(defn permission-list-query
  ([file-id] (permission-list-query file-id {}))
  ([file-id extra-params]
   (let [default-fields [:id :role :withLink :type :domain :emailAddress]]
     (merge {:fields default-fields :file-id file-id}
            (or extra-params {})
            {:model :permissions :action :list}))))

(t/ann PermissionListQuery->DriveRequest [Drive PermissionListQuery -> Drive$Permissions$List])
(defn PermissionListQuery->DriveRequest
  [^Drive drive-service, qmap]
  (let [file-id (:file-id qmap)
        request (.list (.permissions drive-service) file-id)]
    (when-let [fields (format-fields-string qmap)] (.setFields request fields))
    request))

;; Adding a permission to a file/folder

(t/ann ^:no-check permission-insert-query (t/IFn [FileId t/Str Role -> PermissionInsertQuery]
                                                 [FileId t/Str Role (t/HMap) -> PermissionInsertQuery]))
(defn permission-insert-query
  ([file-id principal role]
   (permission-insert-query file-id principal role {}))
  ([file-id principal role extra-params]
   (merge extra-params
          {:model   :permissions
           :action  :insert
           :file-id file-id
           :value   principal
           :role    role
           :type    (guess-principal-type principal)})))


(t/ann PermissionInsertQuery->DriveRequest [Drive PermissionInsertQuery -> Drive$Permissions$Insert])
(defn PermissionInsertQuery->DriveRequest
  [^Drive drive-service, qmap]
  (let [{:keys [file-id value role type with-link?]} qmap
        permission (new Permission)]
    ;build up the Java permission object
    (.setValue permission value)
    (when role (.setRole permission (name role)))
    (when type (.setType permission (name type)))
    (when (some? with-link?) (.setWithLink permission (boolean with-link?)))
    ;now make the request
    (let [permission-svc (.permissions drive-service)
          request (.insert permission-svc file-id permission)]
      ;TODO: make sendNotificationEmails configurable via qmap?
      (.setSendNotificationEmails request false)
      (when-let [fields (format-fields-string qmap)] (.setFields request fields))
      request)))

;; Alter a permission on a file/folder

(t/ann permission-update-query (t/IFn [FileId PermissionId Role -> PermissionUpdateQuery]
                                      [FileId PermissionId Role (t/HMap) -> PermissionUpdateQuery]))
(defn ^:no-check permission-update-query
  ([file-id permission-id role]
   (permission-update-query file-id permission-id role {}))
  ([file-id permission-id role extra-params]
   (merge extra-params
          {:model         :permissions
           :action        :update
           :file-id       file-id
           :permission-id permission-id
           :role          role})))

(t/ann PermissionUpdateQuery->DriveRequest [Drive PermissionUpdateQuery -> Drive$Permissions$Update])
(defn PermissionUpdateQuery->DriveRequest
  [^Drive drive-service, qmap]
  (let [{:keys [file-id permission-id role transfer-ownership?]} qmap
        permission (new Permission)]
    ;build up the Java permission object
    (.setRole permission (name role))
    ;now make the request
    (let [permission-svc (.permissions drive-service)
          request (.update permission-svc file-id permission-id permission)]
      (when (some? transfer-ownership?) (.setTransferOwnership request (boolean transfer-ownership?)))
      (when-let [fields (format-fields-string qmap)] (.setFields request fields))
      request)))


;; Removing a single permission from a single file/folder

(t/ann permission-delete-query [FileId PermissionId -> PermissionDeleteQuery])
(defn permission-delete-query
  [file-id permission-id]
  {:model         :permissions
   :action        :delete
   :file-id       file-id
   :permission-id permission-id})

(t/ann PermissionDeleteQuery->DriveRequest [Drive PermissionDeleteQuery -> Drive$Permissions$Delete])
(defn PermissionDeleteQuery->DriveRequest
  [^Drive drive-service, qmap]
  (.delete (.permissions drive-service)
           (:file-id qmap)
           (:permission-id qmap)))



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


(t/ann ^:no-check Query->DriveRequest [Drive Query -> Request])
(defn- Query->DriveRequest
  "Converts a Query map into an instance of DriveRequest.  All Query maps have at least
  a `:model` and an `:action` key describing the type of operation that they represent.
  The exact class returned depends on the `:model` and `:action` keys of the Query map,
  but all returend classes are ones that implement DriveRequest."
  [drive-service query]
  (t/let [model :- t/Kw (:model query)
          action :- t/Kw (:action query)]
    (case model
      :files
      (case action
        :list (FileListQuery->DriveRequest drive-service query)
        :get (FileGetQuery->DriveRequest drive-service query)
        :insert (FileInsertQuery->DriveRequest drive-service query)
        :update (FileUpdateQuery->DriveRequest drive-service query)
        :delete (FileDeleteQuery->DriveRequest drive-service query))
      :permissions
      (case action
        :list (PermissionListQuery->DriveRequest drive-service query)
        :insert (PermissionInsertQuery->DriveRequest drive-service query)
        :update (PermissionUpdateQuery->DriveRequest drive-service query)
        :delete (PermissionDeleteQuery->DriveRequest drive-service query)))))


(t/ann ^:no-check response-data [ResponseRaw -> ResponseData])
(defn response-data
  [response]
  (cond
    (instance? FileList response) (.getItems ^FileList response)
    (instance? PermissionList response) (.getItems ^PermissionList response)
    :otherwise response))

(t/ann next-page! [DriveRequest ResponseRaw -> (t/Option Request)])
(defn next-page!
  [request response]
  (cond
    ;file lists can be paginated, so we'll return a new request object
    ;primed with a new page token
    (and (instance? Drive$Files$List request)
         (instance? FileList response))
    (when-let [page-token (.getNextPageToken ^FileList response)]
      (.setPageToken ^Drive$Files$List request page-token)
      request)
    ;nothing else has a notion of having a "next" page
    :otherwise nil))


(t/ann ^:no-check camel->kebab [t/Str -> t/Str])
(defn- camel->kebab
  [^String camel]
  (let [expander (fn [^StringBuffer buf, ^Character ch]
                   (let [lc (Character/toLowerCase ch)]
                     ;if char is uppercase and NOT the first char, add a dash
                     (when (and (not= ch lc) (> (.length buf) 0))
                       (.append buf \-))
                     ;always add the (lowercased) char
                     (.append buf lc)))
        expanded (reduce expander (StringBuffer.) camel)]
    (.toString ^StringBuffer expanded)))


(t/ann
  ^:no-check convert-response
  (t/IFn [java.util.Collection -> (t/Seqable QueryResult)]
         [java.util.Map -> (t/HMap)]
         [java.lang.Number -> java.lang.Number]
         [java.lang.String -> java.lang.String]
         [java.lang.Boolean -> java.lang.Boolean]
         [clojure.lang.Keyword -> clojure.lang.Keyword]
         [java.util.Date -> java.util.Date]
         [com.google.gdata.data.DateTime -> com.google.gdata.data.DateTime]
         [com.google.api.client.util.DateTime -> com.google.api.client.util.DateTime]
         [nil -> nil]))
(defn convert-response
  "Converts a plain Google response object (usually something like com.google.api.client.util.ArrayMap,
  com.google.api.client.json.GenericJson, or a collection of the same) into Clojure-y maps and vecs and whatnot.
  Some types that are already Clojure-friendly will be passed through unchanged (like Number, String, Boolean, nil, etc)
  Maps with string keys will have those strings converted into kebab-case keywords (e.g. \"fooBar\" -> :foo-bar)"
  [pojo]
  (when pojo
    (condp instance? pojo
      ;transform each element of lists
      java.util.Collection (mapv convert-response pojo)
      ;rewrite map keys as kebob keywords
      java.util.Map (->> (keys pojo)
                         (map (fn [^String k]
                                [(if (string? k) (keyword (camel->kebab k)) k)
                                 (convert-response (get pojo k))]))
                         (into {}))
      ;otherwise just pass through pojo unaltered
      pojo)))

(t/ann ^:no-check rate-limit-exceeded? [GoogleJsonError -> t/Bool])
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


(t/ann ^:no-check execute-query! [cred/GoogleAuth Query -> QueryResult])
(defn execute-query!
  "Executes the given query in the google context and returns the
   results converted into clojure forms. If the response is paginated,
   all results are fetched and concatenated into a vector."
  [google-ctx query]
  (let [drive-service (build-drive-service google-ctx)
        request (Query->DriveRequest drive-service query)
        results (atom nil)]
    (loop []
      (let [response (.execute ^DriveRequest request)
            data (convert-response (response-data response))]
        (if (next-page! request response)
          (do
            (swap! results (fn [extant] (into (or extant []) data)))
            (recur))
          (swap! results (fn [extant]
                           (if extant
                             (into extant data)
                             data))))))
    @results))

(t/ann ^:no-check execute-batch! [cred/GoogleAuth (t/Seqable Query) -> (t/Seqable QueryResult)])
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
  (let [drive-service (build-drive-service google-ctx)
        requests (map (partial Query->DriveRequest drive-service) queries)
        credential (cred/build-credential google-ctx)
        batch (BatchRequest. cred/http-transport credential)
        responses (atom (into [] (repeat (count requests) nil)))]
    (loop [requests (map-indexed vector requests)]
      (let [next-requests (atom {})]
        (doseq [[i ^DriveRequest request] requests]
          (.queue request batch GoogleJsonErrorContainer
                  (reify BatchCallback
                    (onSuccess [_ response headers]
                      (let [data (convert-response (response-data response))]
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

(t/ann ^:no-check execute! [cred/GoogleAuth (t/Seqable Query) -> (t/Seqable QueryResult)])
(defn execute!
  "Executes the given queries in the most efficient way, returning their
   results in a seq of clojure forms. Note the queries may be processed
   concurrently."
  [google-ctx queries]
  (when (seq queries)
    (if (= 1 (count queries))
      [(execute-query! google-ctx (first queries))]
      (let [batches (partition-all gdrive-const/batch-size queries)]
        (doall (mapcat (partial execute-batch! google-ctx) batches))))))

;;;; Commands and their helpers

(t/ann ^:no-check get-permissions! (t/IFn [cred/GoogleAuth FileId -> QueryResult]
                                          [cred/GoogleAuth FileId (t/Option t/Str) -> QueryResult]))
(defn get-permissions!
  "Returns the permissions granted on the given file, filtered for those
   explicitly granted to the principal if given"
  ([google-ctx file-id]
   (get-permissions! google-ctx file-id nil))
  ([google-ctx, file-id, ^String principal]
   (let [query (permission-list-query file-id)
         permissions (execute-query! google-ctx query)]
     (if (some? principal)
       (filter #(permission-has-principal? principal %) permissions)
       permissions))))

;TODO: better annotations
(t/ann ^:no-check summarize-permissions [(t/Seqable (t/HMap)) -> (t/HMap)])
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

;TODO: better annotations
(t/ann ^:no-check assign! [cred/GoogleAuth FileId (t/HMap) -> (t/HMap)])
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
        principal-id (atom nil)                             ; TODO this could be a volatile
        ids-to-delete (atom [])]
    ;; [principal withLink] seem to be a unique key within a file
    (doseq [permission extant]
      (if (and (= (name role) (:role permission))
               (case searchable?
                 true (true? (:with-link? permission))
                 false (nil? (:with-link? permission))))
        (reset! principal-id (:id permission))
        (swap! ids-to-delete conj (:id permission))))
    (let [deletes (map (partial permission-delete-query file-id) @ids-to-delete)
          insert (when-not @principal-id (permission-insert-query file-id principal role {:with-link? searchable? :fields [:id]}))]
      (execute! google-ctx deletes)
      (when insert
        (execute-query! google-ctx insert)))
    nil))

;TODO: better annotations
(t/ann ^:no-check revoke! [cred/GoogleAuth FileId t/Str -> nil])
(defn revoke!
  "Revoke all authorizations for the given principal on the given file. The
   principal may be the literal \"anyone\", an email address of a user or
   google app group, or a google app domain.

   This operation should be idempotent until the the permissions change by some
   other operation."
  [google-ctx file-id principal]
  (let [extant (get-permissions! google-ctx file-id principal)
        deletes (mapv #(permission-delete-query file-id (:id %)) extant)]
    (execute! google-ctx deletes)
    nil))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;TODO: better annotations
(t/ann create-folder! [cred/GoogleAuth FolderId t/Str -> QueryResult])
(defn create-folder!
  "Create a folder with the given title in the given parent folder"
  [google-ctx parent-id title]
  (execute-query! google-ctx (folder-insert-query parent-id title)))

;TODO: better annotations
(t/ann move-file! [cred/GoogleAuth FolderId FileId -> QueryResult])
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

;TODO: better annotations
(t/ann ^:no-check download-file! (t/IFn [cred/GoogleAuth (t/HMap) -> QueryResult]
                                        [cred/GoogleAuth (t/HMap) (t/Option t/Str) -> QueryResult]))
(defn download-file!
  "Downloads the contents of the given file as an inputstream, or nil if the
   file is not available or is not available in the given mime type."
  ([google-ctx file]
   (download-file! google-ctx file nil))
  ([google-ctx file mime-type]
   (when (:id file)
     (let [drive-svc (build-drive-service google-ctx)
           files-svc (.files drive-svc)]
       (if mime-type
         (when-let [url (get (:export-links file) mime-type)]
           ;; This is purely to force authentication on the stupid drive request
           ;; factory. There is almost certainly a better way to handle this,
           ;; either locally or by reconsidering the google-ctx object
           (.execute (.setFields (.get files-svc (:id file)) "id"))
           (let [http-request (.getRequestFactory drive-svc)
                 gurl (GenericUrl. ^String url)
                 _ (prn "gurl" gurl)
                 get-request (.buildGetRequest http-request gurl)
                 response (.execute get-request)]
             (.getContent response)))
         (when-let [url (:download-url file)]
           (.executeMediaAsInputStream (.get files-svc ^String (:id file)))))))))


;TODO: better annotations
(t/ann delete-file! [cred/GoogleAuth FileId -> QueryResult])
(defn delete-file!
  "Permanently deletes the given file. If the file is a folder, this also
   deletes all of its descendents."
  [google-ctx file-id]
  (execute-query! google-ctx (file-delete-query file-id)))

;TODO: better annotations
(t/ann list-files! (t/IFn [cred/GoogleAuth FolderId -> QueryResult]
                          [cred/GoogleAuth FolderId (t/HMap) -> QueryResult]))
(defn list-files!
  "Returns a seq of files in the given folder"
  ([google-ctx folder-id]
   (list-files! google-ctx folder-id {}))
  ([google-ctx folder-id extras]
   (execute-query! google-ctx (folder-list-files-query folder-id extras))))

;TODO: better annotations
(t/ann get-file! [cred/GoogleAuth FileId -> QueryResult])
(defn get-file!
  "Returns the metadata for the given file"
  [google-ctx file-id]
  (execute-query! google-ctx (file-get-query file-id)))

;TODO: better annotations
(t/ann ^:no-check find-file! (t/IFn [cred/GoogleAuth FolderId (t/Seqable t/Str) -> QueryResult]
                                    [cred/GoogleAuth FolderId (t/Seqable t/Str) (t/Option FieldList) -> QueryResult]))
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
             query (cond-> {:model  :files
                            :action :list
                            :query  q}
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
