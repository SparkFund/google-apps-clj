(ns google-apps-clj.credentials
  "A library used to set up Google OAuth 2 credentials"
  (:require [clojure.core.typed :as t]
            [clojure.edn :as edn :only [read-string]]
            [clojure.data.json :as json]
            [org.httpkit.client :as http])
  (:import (com.google.api.client.googleapis.auth.oauth2 GoogleClientSecrets
                                                         GoogleClientSecrets$Details
                                                         GoogleCredential$Builder
                                                         GoogleTokenResponse)
           (com.google.api.client.googleapis.javanet GoogleNetHttpTransport)
           (com.google.api.client.http HttpTransport
                                       HttpRequestInitializer)
           (com.google.api.client.json JsonFactory)
           (com.google.api.client.json.jackson2 JacksonFactory)))


(t/defalias AuthMap (t/HMap :mandatory {:access-token  t/Str
                                        :expires-in    t/AnyInteger
                                        :refresh-token t/Str
                                        :token-type    t/Str}
                            :complete? true))

(t/defalias GoogleCtx
  (t/HMap :mandatory {:client-id     t/Str
                      :client-secret t/Str
                      :redirect-uris (t/Vec t/Str)
                      :auth-map      AuthMap}
          :optional {:connect-timeout t/AnyInteger
                     :read-timeout    t/AnyInteger
                     :access-type     t/Str
                     :redirect-uri    t/Str}))
  
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
                  (.setRedirectUris (:redirect-uris google-ctx)))
        google-secret (doto (GoogleClientSecrets.)
                        (.setInstalled details))]
    google-secret))

(defn- get-user-auth-code [google-ctx scope]
  (let [baseurl "https://accounts.google.com/o/oauth2/v2/auth"
        code "response_type=code"
        scope (str "scope=" (clojure.string/join "%20" scope))
        client-id (str "client_id=" (:client-id google-ctx))
        acc-type (str "access_type=" (:access-type google-ctx "offline"))
        redirect_uri (str "redirect_uri=" (:redirect-uri google-ctx "urn:ietf:wg:oauth:2.0:oob"))
        args (str "?" code "&" scope "&" client-id "&" acc-type "&" redirect_uri)
        auth-url (str baseurl args)]
    (println "Please visit the following url and input the code "
             "that appears on the screen: " auth-url)
    (read-line)))

(defn- underscore-to-dash-in-keys [map-in]
  (into {} (map
             (fn [[key val]] [(keyword (clojure.string/replace (name key) "_" "-")) val])
             map-in)))

(t/ann get-auth-map [GoogleCtx -> AuthMap])
(defn get-auth-map
  "Given a google-ctx configuration map, and a list of scopes(as strings),
   creates a URL for the user to receive their auth-code, which is then used
   to receive an authorization map, which the user should store securely"
  [google-ctx scope]
  (let [base-url "https://www.googleapis.com/oauth2/v4/token"
        auth-code (get-user-auth-code google-ctx scope)
        body {:code          auth-code
              :client_id     (:client-id google-ctx)
              :client_secret (:client-secret google-ctx)
              :grant_type    "authorization_code"
              :redirect_uri  (:redirect-uri google-ctx "urn:ietf:wg:oauth:2.0:oob")}
        response (deref (http/post base-url {:form-params body}) 5000 {:status "TIMEOUT AFTER 5 SECONDS"})]
    (if (= 200 (:status response))
      (underscore-to-dash-in-keys (json/read-json (:body response)))
      (println "Authentication request failed, dumping response:" response))))

(t/ann refresh-google-token [GoogleCtx -> AuthMap])
(defn refresh-google-token
  "Given a google-ctx configuration map containing a refresh token in the :auth-map,
   returns an updated :auth-map with a refreshed access-token."
  [google-ctx]
  (let [base-url "https://www.googleapis.com/oauth2/v4/token"
        refresh-token (get-in google-ctx [:auth-map :refresh-token])
        body {:refresh_token refresh-token
              :client_id     (:client-id google-ctx)
              :client_secret (:client-secret google-ctx)
              :grant_type    "refresh_token"}
        response (deref (http/post base-url {:form-params body}) 5000 {:status "TIMEOUT AFTER 5 SECONDS"})]
    (if (= 200 (:status response))
      (assoc (underscore-to-dash-in-keys (json/read-json (:body response))) :refresh-token refresh-token)
      (println "Token refresh request failed, dumping response:" response))))

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

(t/ann ^:no-check build-credential [GoogleCtx -> HttpRequestInitializer])
(defn build-credential
  "Given a google-ctx configuration map, builds a GoogleCredential Object from
   the token response and google secret created from those respective methods."
  [google-ctx]
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
      credential)))
