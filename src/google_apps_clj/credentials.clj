(ns google-apps-clj.credentials
  "A library used to set up Google OAuth 2 credentials"
  (:require [clojure.core.typed :as t]
            [clojure.java.io :as io]
            [clojure.edn :as edn :only [read-string]])
  (:import (com.google.api.client.auth.oauth2 TokenResponse)
           (com.google.api.client.googleapis.auth.oauth2 GoogleAuthorizationCodeFlow$Builder
                                                         GoogleClientSecrets
                                                         GoogleClientSecrets$Details
                                                         GoogleCredential
                                                         GoogleCredential$Builder
                                                         GoogleTokenResponse)
           (com.google.api.client.googleapis.javanet GoogleNetHttpTransport)
           (com.google.api.client.http HttpTransport
                                       HttpRequestInitializer)
           (com.google.api.client.json JsonFactory)
           (com.google.api.client.json.jackson2 JacksonFactory)
           (java.io ByteArrayInputStream)
           (java.nio.charset Charset)))

(t/defalias GoogleCtx
  (t/HMap :mandatory {:client-id t/Str
                      :client-secret t/Str
                      :redirect-uris (t/Seqable t/Str)
                      :auth-map (t/HMap :mandatory {:access-token t/Str
                                                    :expires-in t/AnyInteger
                                                    :refresh-token t/Str
                                                    :token-type t/Str}
                                        :complete? true)}
          :optional {:connect-timeout t/AnyInteger
                     :read-timeout t/AnyInteger}))

(t/defalias GoogleAuth (t/U GoogleCtx GoogleCredential))
(t/defalias OAuthScopes (t/Coll t/Str))

(t/non-nil-return com.google.api.client.json.jackson2.JacksonFactory/getDefaultInstance :all)
(t/non-nil-return com.google.api.client.googleapis.javanet.GoogleNetHttpTransport/newTrustedTransport :all)
(t/non-nil-return com.google.api.client.googleapis.auth.oauth2.GoogleCredential/createScoped :all)
(t/non-nil-return com.google.api.client.googleapis.auth.oauth2.GoogleCredential/getApplicationDefault :all)
(t/non-nil-return com.google.api.client.googleapis.auth.oauth2.GoogleCredential/fromStream :all)
(t/non-nil-return java.nio.charset.Charset/forName :all)

(t/ann ^:no-check clojure.java.io/input-stream [t/Any -> java.io.InputStream])
(t/ann ^:no-check clojure.core/some? [t/Any -> t/Bool])

(t/ann http-transport HttpTransport)
(def ^HttpTransport http-transport (GoogleNetHttpTransport/newTrustedTransport))

(t/ann json-factory JsonFactory)
(def ^JsonFactory json-factory (JacksonFactory/getDefaultInstance))


(t/ann get-google-secret [GoogleCtx -> GoogleClientSecrets])
(defn get-google-secret
  "Given a google-ctx configuration map, creates a GoogleClientSecrets Object
   with the client-id, client secret, and redirect uris pulled from the config"
  [google-ctx]
  (let [details (doto (GoogleClientSecrets$Details.)
                  (.setClientId (:client-id google-ctx))
                  (.setClientSecret (:client-secret google-ctx))
                  (.setRedirectUris (vec (:redirect-uris google-ctx))))
        google-secret (doto (GoogleClientSecrets.)
                        (.setInstalled details))]
    google-secret))

(t/ann get-auth-map [GoogleCtx (java.util.Collection String) -> TokenResponse])
(defn get-auth-map
  "Given a google-ctx configuration map, and a list of scopes(as strings),
   creates a URL for the user to receive their auth-code, which is then used
   to receive an authorization map, which the user should store securely"
  [google-ctx scope]
  (let [google-secret (get-google-secret google-ctx)
        auth-flow-builder (doto (GoogleAuthorizationCodeFlow$Builder. http-transport json-factory
                                                                      google-secret scope)
                            (.setAccessType "offline"))
        auth-flow (doto (.build auth-flow-builder)
                    assert)
        auth-request-url (doto (.newAuthorizationUrl auth-flow)
                           assert
                           (.setRedirectUri "urn:ietf:wg:oauth:2.0:oob"))
        auth-url (.build auth-request-url)
        _ (println "Please visit the following url and input the code "
                   "that appears on the screen: " auth-url)
        auth-code (doto ^String (read-line) assert)
        token-request (doto (.newTokenRequest auth-flow auth-code)
                        assert
                        (.setRedirectUri "urn:ietf:wg:oauth:2.0:oob"))]
    (doto (.execute token-request)
      assert)))

(t/ann get-token-response [GoogleCtx -> GoogleTokenResponse])
(defn ^GoogleTokenResponse get-token-response
  "Given a google-ctx configuration map, creates a GoogleTokenResponse Object
   by pulling data from the authorization map inside of the google-ctx"
  [google-ctx]
  (let [auth-map (:auth-map google-ctx)
        access-token ^String (:access-token auth-map)
        refresh-token ^String (:refresh-token auth-map)
        token-type ^String (:token-type auth-map)]
    (doto (GoogleTokenResponse.)
      (.setAccessToken access-token)
      (.setRefreshToken refresh-token)
      (.setTokenType token-type))))

(t/ann credential-with-scopes [GoogleCredential OAuthScopes -> GoogleCredential])
(defn ^GoogleCredential credential-with-scopes
  "Creates a copy of the given credential, with the specified scopes attached.
  `scopes` should be a list or vec of one or more Strings"
  [^GoogleCredential cred, scopes]
  (.createScoped cred (set scopes)))

(t/ann credential-from-json-stream [t/Any -> GoogleCredential])
(defn ^GoogleCredential credential-from-json-stream
  "Consumes an input stream containing JSON describing a Google API credential
  `stream` can be anything that can be handled by `clojure.java.io/input-stream`"
  [stream]
  (with-open [input-stream (io/input-stream stream)]
    (GoogleCredential/fromStream input-stream)))

(t/ann credential-from-json [t/Str -> GoogleCredential])
(defn ^GoogleCredential credential-from-json
  "Builds a GoogleCredential from a raw JSON string describing a Google API credential"
  [^String cred-json]
  (let [charset (Charset/forName "UTF-8")
        byte-array (.getBytes cred-json charset)
        input-stream (new ByteArrayInputStream byte-array)]
    (credential-from-json-stream input-stream)))

(t/ann GAPP_CRED_VAR t/Str)
(def ^:private GAPP_CRED_VAR "GOOGLE_APPLICATION_CREDENTIALS")

(t/ann default-credential (t/IFn [-> GoogleCredential] [OAuthScopes -> GoogleCredential]))
(defn default-credential
  "Gets the default credential as configured by the GOOGLE_APPLICATION_CREDENTIALS environment variable
  (see https://developers.google.com/identity/protocols/application-default-credentials)
  Optionally you may specify a collection (list/vec/set) of string scopes to attach to the credential"
  ([]
   (let [prop-path (System/getProperty GAPP_CRED_VAR)
         env-path (System/getenv GAPP_CRED_VAR)]
     (cond
       (some? prop-path) (credential-from-json-stream prop-path)
       (some? env-path) (credential-from-json-stream env-path)
       :otherwise (GoogleCredential/getApplicationDefault))))
  ([scopes]
   (credential-with-scopes (default-credential) (set scopes))))

(t/ann ^:no-check request-initializer [(t/U nil t/Int) (t/U nil t/Int) -> HttpRequestInitializer])
(defn- ^HttpRequestInitializer request-initializer
  "Constructs an instance of HttpRequestInitializer that will set the
  specified timeouts (in ms).  Either or both timeout may be `nil`"
  [connect-timeout read-timeout]
  (reify HttpRequestInitializer
    (initialize [_ request]
      (when connect-timeout (.setConnectTimeout request connect-timeout))
      (when read-timeout (.setReadTimeout request read-timeout)))))

(t/ann build-credential-from-ctx [GoogleCtx -> GoogleCredential])
(defn- build-credential-from-ctx
  "Constructs a GoogleCredential from the token response and Google secret as obtained
  from those respsective methods."
  [google-ctx]
  (let [token-response (get-token-response google-ctx)
        google-secret (get-google-secret google-ctx)
        req-initializer (request-initializer (:connect-timeout google-ctx) (:read-timeout google-ctx))
        credential-builder (doto (GoogleCredential$Builder.)
                             (.setTransport http-transport)
                             (.setJsonFactory json-factory)
                             (.setRequestInitializer req-initializer)
                             (.setClientSecrets google-secret))]
    (doto (.build credential-builder)
      assert
      (.setFromTokenResponse token-response))))

(t/ann build-credential [GoogleAuth -> GoogleCredential])
(defn build-credential
  "Given a google-ctx configuration map, builds a GoogleCredential Object from
   the token response and google secret created from those respective methods.
   If an instance of GoogleCredential is provided, it will be returned unmodified"
  [google-ctx]
  (cond
    ;pass through instances of GoogleCredential
    (instance? GoogleCredential google-ctx)
    google-ctx
    ;construct the credential from the provided context
    :otherwise
    (build-credential-from-ctx google-ctx)))
