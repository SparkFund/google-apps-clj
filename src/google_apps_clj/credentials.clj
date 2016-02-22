(ns google-apps-clj.credentials
  "A library used to set up Google OAuth 2 credentials"
  (:require [clojure.core.typed :as t]
            [clojure.edn :as edn :only [read-string]])
  (:import (com.google.api.client.auth.oauth2 Credential
                                              TokenResponse)
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
           (com.google.api.client.json.jackson2 JacksonFactory)))

(t/defalias GoogleCtx
  (t/HMap :mandatory {:client-id t/Str
                      :client-secret t/Str
                      :redirect-uris (t/Seq t/Str)
                      :auth-map (t/HMap :mandatory {:access-token t/Str
                                                    :expires-in t/AnyInteger
                                                    :refresh-token t/Str
                                                    :token-type t/Str}
                                        :complete? true)}
          :optional {:connect-timeout t/AnyInteger
                     :read-timeout t/AnyInteger}))

(t/non-nil-return com.google.api.client.json.jackson2.JacksonFactory/getDefaultInstance :all)
(t/non-nil-return com.google.api.client.googleapis.javanet.GoogleNetHttpTransport/newTrustedTransport :all)

(t/ann http-transport HttpTransport)
(def http-transport (GoogleNetHttpTransport/newTrustedTransport))

(t/ann json-factory JsonFactory)
(def json-factory (JacksonFactory/getDefaultInstance))


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
        auth-code (doto (read-line)
                    assert)
        token-request (doto (.newTokenRequest auth-flow auth-code)
                        assert
                        (.setRedirectUri "urn:ietf:wg:oauth:2.0:oob"))]
    (doto (.execute token-request)
      assert)))

(t/ann get-token-response [GoogleCtx -> GoogleTokenResponse])
(defn get-token-response
  "Given a google-ctx configuration map, creates a GoogleTokenResponse Object
   by pulling data from the authorization map inside of the google-ctx"
  [google-ctx]
  (let [auth-map (:auth-map google-ctx)]
    (doto (GoogleTokenResponse.)
      (.setAccessToken (:access-token auth-map))
      (.setRefreshToken (:refresh-token auth-map))
      (.setTokenType (:token-type auth-map)))))

(defn ^GoogleCredential credential-with-scopes
  "Creates a copy of the given credential, with the specified scopes attached.
  `scopes` should be a list or vec of one or more Strings"
  [^GoogleCredential cred, scopes]
  (.createScoped cred (set scopes)))

(defn ^GoogleCredential default-credential
  "Gets the default credential as configured by the GOOGLE_APPLICATION_CREDENTIALS environment variable
  (see https://developers.google.com/identity/protocols/application-default-credentials)
  Optionally you may specify a collection (list/vec/set) of string scopes to attach to the credential"
  ([]
   (GoogleCredential/getApplicationDefault))
  ([scopes]
   (credential-with-scopes (default-credential) (set scopes))))

(t/ann ^:no-check build-credential [GoogleCtx -> HttpRequestInitializer])
(defn build-credential
  "Given a google-ctx configuration map, builds a GoogleCredential Object from
   the token response and google secret created from those respective methods.
   If the provided configuration map is the keyword `:default`, this will return the default
   credential as configured by the GOOGLE_APPLICATION_CREDENTIALS environment variable
   (see https://developers.google.com/identity/protocols/application-default-credentials)"
  [google-ctx]
   (cond
     ;pass through instances of GoogleCredential
     (instance? GoogleCredential google-ctx)
     google-ctx
     ;allow :default keyword to allow the default
     (= :default google-ctx)
     (default-credential)
     ;construct the credential from the provided context
     :otherwise
     (let [token-response (get-token-response google-ctx)
           google-secret (get-google-secret google-ctx)
           credential-builder (doto (GoogleCredential$Builder.)
                                (.setTransport http-transport)
                                (.setJsonFactory json-factory)
                                (.setClientSecrets google-secret))
           credential (doto (.build credential-builder)
                        assert
                        (.setFromTokenResponse token-response))
           {:keys [connect-timeout read-timeout]} google-ctx]
       (if (or connect-timeout read-timeout)
         (reify HttpRequestInitializer
           (initialize [_ request]
             (.initialize credential request)
             (when connect-timeout
               (.setConnectTimeout request connect-timeout))
             (when read-timeout
               (.setReadTimeout request read-timeout))))
         credential))))
