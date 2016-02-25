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

(t/ann build-drive-service [cred/GoogleAuth -> Drive])
(defn ^Drive build-drive-service
  "Given a google-ctx configuration map, builds a Drive service using
   credentials coming from the OAuth2.0 credential setup inside google-ctx"
  [google-ctx]
  (let [drive-builder (->> google-ctx
                           cred/build-credential
                           (Drive$Builder. cred/http-transport cred/json-factory))]
    (.setApplicationName drive-builder "google-apps-clj")
    (cast Drive (doto (.build drive-builder)
                  assert))))

(t/defalias FileId t/Str)

;; TODO union type of anything clojure.java.io/input-stream allows
(t/defalias FileUploadContent t/Any)

(t/defalias Fields (t/Seq (t/U t/Keyword t/Str)))

(t/defalias PermissionIdentifier t/Str)

(t/defalias Role (t/U ':owner ':writer ':reader))

(t/defalias PermissionType (t/U ':user ':group ':domain ':anyone))

(t/defalias FileDeleteQuery
  (t/HMap :mandatory {:model ':files
                      :action ':delete
                      :file-id FileId}
          :complete? true))

(t/defalias FileGetQuery
  (t/HMap :mandatory {:model ':files
                      :action ':get
                      :file-id FileId}
          :optional {:fields Fields}
          :complete? true))

(t/defalias FileInsertQuery
  (t/HMap :mandatory {:model ':files
                      :action ':insert
                      :description t/Str
                      :title t/Str}
          :optional {:fields Fields
                     :parent-ids (t/Seq FileId)
                     :writers-can-share? t/Bool
                     :mime-type (t/Option t/Str)
                     :content FileUploadContent
                     :size Long} ; TODO non-negative integer
          :complete? true))

(t/defalias FileListQuery
  (t/HMap :mandatory {:model ':files
                      :action ':list}
          :optional {:query t/Str
                     :fields Fields}
          :complete? true))

(t/defalias FileUpdateQuery
  (t/HMap :mandatory {:model ':files
                      :action ':update
                      :file-id FileId}
          :optional {:fields Fields
                     :parent-ids (t/Seq FileId)
                     :writers-can-share? t/Bool
                     :content FileContent
                     :size t/Int}
          :complete? true))

(t/defalias PermissionDeleteQuery
  (t/HMap :mandatory {:model ':permissions
                      :action ':delete
                      :file-id FileId
                      :permission-id PermissionIdentifier}
          :complete? true))

(t/defalias PermissionInsertQuery
  (t/HMap :mandatory {:model ':permissions
                      :action ':insert
                      :file-id FileId
                      :role Role
                      :type PermissionType
                      :value t/Str}
          :optional {:with-link? t/Bool
                     :fields Fields}
          :complete? true))

(t/defalias PermissionListQuery
  (t/HMap :mandatory {:model ':permissions
                      :action ':list
                      :file-id FileId}
          :optional {:fields Fields}
          :complete? true))

(t/defalias PermissionUpdateQuery
  (t/HMap :mandatory {:model ':permissions
                      :action ':update
                      :file-id FileId
                      :permission-id PermissionIdentifier
                      :role Role}
          :optional {:fields Fields
                     :with-link? t/Bool
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

(t/ann root-id t/Str)
(def root-id
  "The id of the root folder"
  "root")

(t/ann build-file [(t/U FileInsertQuery FileUpdateQuery) -> File])
(defn- ^File build-file
  [query]
  (let [{:keys [description mime-type parent-ids title writers-can-share?]} query
        parents (when (seq parent-ids)
                  (map (t/fn [id :- FileId]
                         (doto (ParentReference.)
                           (.setId id)))
                       parent-ids))]
    (cond-doto (File.)
      description (.setDescription description)
      mime-type (.setMimeType mime-type)
      parents (.setParents parents)
      title (.setTitle title)
      (not (nil? writers-can-share?)) (.setWritersCanShare writers-can-share?))))

(t/ann build-stream [(t/U FileInsertQuery FileUpdateQuery) -> (t/Option InputStreamContent)])
(defn- build-stream
  [query]
  (when-let [content (:content query)]
    (let [mime-type (:mime-type query)
          size (:size query)
          input-stream (io/input-stream content)
          content-stream (new InputStreamContent (or mime-type "") input-stream)]
      (when (integer? size) (.setLength content-stream (long size)))
      content-stream)))

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
        :delete
        (let [{:keys [file-id]} query]
          (.delete (.files drive) file-id))
        :list
        (let [{:keys [query]} query
              request (cond-doto (.list (.files drive))
                        query (.setQ query))]
          (cond-doto ^DriveRequest request
            fields (.setFields fields)))
        :get
        (let [{:keys [file-id]} query
              request (.get (.files drive) file-id)]
          (cond-doto ^DriveRequest request
            fields (.setFields fields)))
        :update
        (let [{:keys [file-id]} query
              file (build-file query)
              stream (build-stream query)
              request (if stream
                        (.update (.files drive) file-id file stream)
                        (.update (.files drive) file-id file))]
          (cond-doto ^DriveRequest request
            fields (.setFields fields)))
        :insert
        (let [file (build-file query)
              stream (build-stream query)
              convert (boolean (get-in query [:convert] true))
              request (if stream
                        (doto (.insert (.files drive) file stream)
                          (.setConvert convert))
                        (.insert (.files drive) file))]
          (cond-doto ^DriveRequest request
            fields (.setFields fields))))
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

(def folder-mime-type
  "application/vnd.google-apps.folder")

(t/ann create-folder [FileId t/Str -> FileInsertQuery])
(defn create-folder
  [parent-id title]
  {:model :files
   :action :insert
   :parent-ids [parent-id]
   :mime-type folder-mime-type
   :title title})

(defn create-folder!
  "Create a folder with the given title in the given parent folder"
  [google-ctx parent-id title]
  (execute-query! google-ctx (create-folder parent-id title)))

(t/ann move-file [FileId FileId -> FileUpdateQuery])
(defn move-file
  [folder-id file-id]
  {:model :files
   :action :update
   :parent-ids [folder-id]
   :file-id file-id})

(defn move-file!
  "Moves a file to a folder. This returns true if successful, false
   if forbidden, and raises otherwise."
  [google-ctx folder-id file-id]
  (try
    (execute-query! google-ctx (move-file folder-id file-id))
    true
    (catch GoogleJsonResponseException e
      (when (not= 400 (.getStatusCode e))
        (throw e))
      false)))

(t/ann upload-file [FileId t/Str t/Str t/Str t/Any -> FileInsertQuery])
(defn upload-file
  ([folder-id title description mime-type content]
   (upload-file folder-id title description mime-type content nil))
  ([folder-id title description mime-type content convert-to-gdocs]
   {:model       :files
    :action      :insert
    :parent-ids  [folder-id]
    :title       title
    :description description
    :mime-type   mime-type
    :convert     (cond
                   (= true convert-to-gdocs) true
                   (= false convert-to-gdocs) false
                   :else (some? mime-type))
    :content     content}))

(defn upload-file!
  "Uploads a file with the given title, description, type, and content into
   the given folder"
  ([google-ctx folder-id title description mime-type content]
   (upload-file! google-ctx folder-id title description mime-type content true))
  ([google-ctx folder-id title description mime-type content convert-to-gdocs]
   (execute-query! google-ctx
                   (upload-file folder-id title description mime-type content convert-to-gdocs))))

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

(t/ann delete-file [FileId -> FileDeleteQuery])
(defn delete-file
  [file-id]
  {:model :files
   :action :delete
   :file-id file-id})

(defn delete-file!
  "Permanently deletes the given file. If the file is a folder, this also
   deletes all of its descendents."
  [google-ctx file-id]
  (execute-query! google-ctx (delete-file file-id)))

(t/ann list-files [FileId -> FileListQuery])
(defn list-files
  [folder-id]
  {:model :files
   :action :list
   :query (format "'%s' in parents" folder-id)})

(defn list-files!
  "Returns a seq of files in the given folder"
  [google-ctx folder-id]
  (execute-query! google-ctx (list-files folder-id)))

(t/ann get-file [FileId -> FileGetQuery])
(defn get-file
  [file-id]
  {:model :files
   :action :get
   :file-id file-id})

(defn get-file!
  "Returns the metadata for the given file"
  [google-ctx file-id]
  (execute-query! google-ctx (get-file file-id)))

;; TODO core.typed should complain that not all Query types have :fields?
(t/ann with-fields [Query -> Query])
(defn with-fields
  "Sets or adds to the set of fields returned by the given request"
  [query fields]
  (update-in query [:fields] (fnil into #{}) fields))

(t/ann all-files Query)
(def all-files
  (let [fields [:id :title :writersCanShare :mimeType
                "permissions(emailAddress,type,domain,role,withLink)"
                "owners(emailAddress)"
                "parents(id)"]]
    {:model :files
     :action :list
     :fields fields
     :query "trashed=false"}))

(t/ann folder? [(t/HMap :mandatory {:mime-type t/Str}) -> t/Bool])
(defn folder?
  "Predicate fn indicating if the given file map has the folder mime type"
  [file]
  (= folder-mime-type (:mime-type file)))

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
