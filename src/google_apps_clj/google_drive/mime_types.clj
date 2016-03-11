(ns google-apps-clj.google-drive.mime-types
  "Documentation of the supported custom Google Drive mime-types
  (from https://developers.google.com/drive/v3/web/mime-types)"
  (:require [clojure.core.typed :as t]))

(t/def audio        :- t/Str "application/vnd.google-apps.audio")
(t/def document     :- t/Str "application/vnd.google-apps.document")
(t/def drawing      :- t/Str "application/vnd.google-apps.drawing")
(t/def file         :- t/Str "application/vnd.google-apps.file")
(t/def folder       :- t/Str "application/vnd.google-apps.folder")
(t/def form         :- t/Str "application/vnd.google-apps.form")
(t/def fusion-table :- t/Str "application/vnd.google-apps.fusiontable")
(t/def map-custom   :- t/Str "application/vnd.google-apps.map")
(t/def photo        :- t/Str "application/vnd.google-apps.photo")
(t/def presentation :- t/Str "application/vnd.google-apps.presentation")
(t/def apps-script  :- t/Str "application/vnd.google-apps.script")
(t/def sites        :- t/Str "application/vnd.google-apps.sites")
(t/def spreadsheet  :- t/Str "application/vnd.google-apps.spreadsheet")
(t/def unknown      :- t/Str "application/vnd.google-apps.unknown")
(t/def video        :- t/Str "application/vnd.google-apps.video")
