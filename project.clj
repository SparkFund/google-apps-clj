(defproject google-apps-clj "0.1.0"
  :description "A Clojure library that wraps the Google Java API"
  :url "https://github.com/dunn-mat/google-apps-clj"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/core.typed "0.3.0-alpha2"]
                 [com.google.gdata/core "1.47.1"]
                 [com.google.apis/google-api-services-drive "v2-rev168-1.20.0"]]
  :repositories [["releases" {:url "https://github.com/dunn-mat/google-apps-clj"
                              :creds :gpg}]]
  :deploy-repositories [["releases" :clojars]])
