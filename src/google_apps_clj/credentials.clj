(ns google-apps-clj.credentials
  "A library used to set up Google OAuth 2 credentials"
  (:require [clojure.edn :as edn :only [read-string]]
            [clojure.core.typed :as t])
  (:import (com.google.api.client.googleapis.auth.oauth2 GoogleCredential
                                                         GoogleCredential$Builder
                                                         GoogleClientSecrets
                                                         GoogleAuthorizationCodeFlow$Builder
                                                         GoogleClientSecrets$Details
                                                         GoogleTokenResponse)
           (com.google.api.client.googleapis.javanet GoogleNetHttpTransport)
           (com.google.api.client.http.javanet NetHttpTransport)
           (com.google.api.client.json.jackson2 JacksonFactory)))

(t/defalias GoogleCtx '{:client-id String
                        :client-secret String
                        :redirect-uris '[String]
                        :auth-map '{:access-token String
                                    :expires-in Number
                                    :refresh-token String
                                    :token-type String}})
(t/non-nil-return com.google.api.client.json.jackson2.JacksonFactory/getDefaultInstance :all)
(t/non-nil-return com.google.api.client.googleapis.javanet.GoogleNetHttpTransport/newTrustedTransport :all)
;(t/override-constructor GoogleClientSecrets$Details. GoogleClientSecrets$Details)

(t/ann http-transport NetHttpTransport)
(def http-transport (GoogleNetHttpTransport/newTrustedTransport))

(t/ann json-factory JacksonFactory)
(def json-factory (JacksonFactory/getDefaultInstance))

(t/ann get-google-secret [GoogleCtx -> GoogleClientSecrets])
(defn get-google-secret
  "Given a google-ctx configuration map, creates a GoogleClientSecrets Object
   with the client-id, client secret, and redirect uris pulled from the config"
  [google-ctx]
  (let [details (-> (GoogleClientSecrets$Details.)
                    (.setClientId (:client-id google-ctx))
                    (.setClientSecret (:client-secret google-ctx))
                    (.setRedirectUris (:redirect-uris google-ctx)))
        _ (assert details)
        google-secret (-> (GoogleClientSecrets.)
                          (.setInstalled details))]
    (assert google-secret)
    google-secret))

(t/ann ^:no-check get-auth-map [GoogleCtx (t/Seq String) -> t/Map])
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
        auth-code (read-line)
        _ (assert auth-code)]
    (-> auth-flow
        (.newTokenRequest auth-code)
        (.setRedirectUri "urn:ietf:wg:oauth:2.0:oob")
        .execute)))

(t/ann ^:no-check get-token-response [GoogleCtx -> GoogleTokenResponse])
(defn get-token-response
  "Given a google-ctx configuration map, creates a GoogleTokenResponse Object
   by pulling data from the authorization map inside of the google-ctx"
  [google-ctx]
  (let [auth-map (:auth-map google-ctx)]
    (-> (GoogleTokenResponse.)
        (.setAccessToken (:access-token auth-map))
        (.setRefreshToken (:refresh-token auth-map))
        (.setTokenType (:token-type auth-map)))))

(t/ann ^:no-check build-credential [GoogleCtx -> GoogleCredential])
(defn build-credential
  "Given a google-ctx configuration map, builds a GoogleCredential Object from 
   the token response and google secret created from those respective methods.
   Uses the default JSON factory and GoogleHttpTransport"
  [google-ctx]
  (let [token-response (get-token-response google-ctx)
        google-secret (get-google-secret google-ctx)
        credential (-> (GoogleCredential$Builder.)
                       (.setTransport http-transport)
                       (.setJsonFactory json-factory)
                       (.setClientSecrets google-secret)
                       .build)
        _ (assert credential)
        _ (do (.setFromTokenResponse credential token-response))]
    credential))
