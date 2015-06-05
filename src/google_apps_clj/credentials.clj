(ns google-apps-clj.credentials
  "A library used to set up Google OAuth 2 credentials"
  (:require [clojure.edn :as edn :only [read-string]])
  (:import (com.google.api.client.googleapis.auth.oauth2 GoogleCredential$Builder
                                                         GoogleClientSecrets
                                                         GoogleAuthorizationCodeFlow$Builder
                                                         GoogleClientSecrets$Details
                                                         GoogleTokenResponse)
           (com.google.api.client.googleapis.javanet GoogleNetHttpTransport)
           (com.google.api.client.json.jackson2 JacksonFactory)))

(def http-transport (GoogleNetHttpTransport/newTrustedTransport))
(def json-factory (JacksonFactory/getDefaultInstance))

(defn get-google-secret
  "Given a google-ctx configuration map, creates a GoogleClientSecrets Object
   with the client-id, client secret, and redirect uris pulled from the config"
  [google-ctx]
  (let [details (-> (GoogleClientSecrets$Details.)
                    (.setClientId (:client-id google-ctx))
                    (.setClientSecret (:client-secret google-ctx))
                    (.setRedirectUris (:redirect-uris google-ctx)))]
    (-> (GoogleClientSecrets.)
        (.setInstalled details))))

(defn get-auth-map
  "Given a google-ctx configuration map, and a list of scopes(as strings),
   creates a URL for the user to go to in order to recieve their
   authorization code for get-auth-map"
  [google-ctx scope]
  (let [google-secret (get-google-secret google-ctx)
        auth-flow (-> (GoogleAuthorizationCodeFlow$Builder. http-transport json-factory
                                                            google-secret scope)
                      (.setAccessType "offline")
                      .build)
        url-for-auth-code (-> auth-flow
                              .newAuthorizationUrl
                              (.setRedirectUri "urn:ietf:wg:oauth:2.0:oob")
                              .build)
        _ (println "Please visit the following url and input the code "
                   "that appears on the screen: " url-for-auth-code)
        auth-code (read-line)]
    (-> auth-flow
        (.newTokenRequest auth-code)
        (.setRedirectUri "urn:ietf:wg:oauth:2.0:oob")
        .execute)))

(defn get-token-response
  "Given a google-ctx configuration map, creates a GoogleTokenResponse Object
   by pulling data from the authorization map inside of the google-ctx"
  [google-ctx]
  (let [auth-map (:auth-map google-ctx)]
    (-> (GoogleTokenResponse.)
        (.setAccessToken (get auth-map "access_token"))
        (.setRefreshToken (get auth-map "refresh_token"))
        (.setTokenType (get auth-map "token_type")))))
