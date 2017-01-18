(defproject google-apps-clj "0.6.1"
  :description "A Clojure library that wraps the Google Java API"
  :url "https://github.com/SparkFund/google-apps-clj"
  :license {:name "Apache License, Version 2.0"
            :url "http://www.apache.org/licenses/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/core.typed "0.3.14"]
                 [clj-time "0.12.2"]
                 [com.google.apis/google-api-services-calendar "v3-rev202-1.22.0"]
                 [com.google.apis/google-api-services-drive "v2-rev168-1.20.0"]
                 [com.google.apis/google-api-services-sheets "v4-rev12-1.22.0"]
                 [com.google.oauth-client/google-oauth-client-jetty "1.22.0"]
                 [com.google.gdata/core "1.47.1" :exclusions [org.apache.httpcomponents/httpclient
                                                              org.mortbay.jetty/jetty
                                                              com.google.code.findbugs/jsr305]]]
  :plugins [[lein-typed "0.3.5"]]
  :repl-options {:init-ns google-apps-clj.repl}
  :test-selectors {:integration :integration
                   :all (constantly true)
                   :default (complement :integration)}
  :core.typed {:check [google-apps-clj.credentials
                       google-apps-clj.google-calendar
                       google-apps-clj.google-drive
                       google-apps-clj.google-sheets]}
  :pedantic? :abort)
