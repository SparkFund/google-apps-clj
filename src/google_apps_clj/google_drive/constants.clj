(ns google-apps-clj.google-drive.constants
  "Documentation of miscellaneous Google Drive constants"
  (:require [clojure.core.typed :as t]))

(t/def batch-size
  "Google internally unwraps batches and processes them concurrently. Batches
   that are too large can cause the request to exceed Google's api rate limit,
   which is applied to api requests, not http requests."
  :- Long
  20)

(t/def root-folder-id
  "The unique ID of the root folder of Google Drive"
  :- t/Str
  "root")
