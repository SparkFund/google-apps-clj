(ns google-apps-clj.google-calendar
  (:require [clojure.core.typed :as t]
            [clojure.core.typed.unsafe :as tu]
            [clojure.edn :as edn :only [read-string]]
            [clojure.java.io :as io :only [as-url file resource]]
            [google-apps-clj.credentials :as cred])
  (:import (com.google.api.client.util DateTime)
           (com.google.api.services.calendar.model Event
                                                   EventAttendee
                                                   EventDateTime)
           (com.google.api.services.calendar Calendar
                                             Calendar$Builder
                                             Calendar$Events$Insert
                                             CalendarScopes)))

(t/ann build-calendar-service [cred/GoogleCtx -> Calendar])
(defn build-calendar-service
  "Given a google-ctx configuration map, builds a Calendar service using
  credentials coming from the OAuth2.0 credential setup inside googlectx"
  [google-ctx]
  (let [calendar-builder (->> google-ctx
                           cred/build-credential
                           (Calendar$Builder. cred/http-transport cred/json-factory))]
    (cast Calendar (doto (.build calendar-builder)
                     assert))))

(t/ann add-calendar-event [cred/GoogleCtx String String String String String (t/Coll String) -> t/Any])
(defn add-calendar-event
  ""
  [google-ctx summary description location start-time end-time attendees]
  (let [calendar-service (build-calendar-service google-ctx)
        events (doto (.events ^ Calendar calendar-service)
                 assert)
        event (doto (Event.)
                (.setSummary summary)
                (.setLocation location)
                (.setDescription description))
        start-time (DateTime. ^String start-time)
        start (doto (EventDateTime.)
                (.setDateTime start-time))
        end-time (DateTime. ^String end-time)
        end (doto (EventDateTime.)
                (.setDateTime end-time))
        attendees (tu/ignore-with-unchecked-cast
                   (map #(doto (EventAttendee.)
                           (.setEmail %)) attendees)
                   (java.util.List EventAttendee))
        event (doto event
                (.setAttendees attendees)
                (.setStart start)
                (.setEnd end))
        insert-request (doto (.insert events "primary" event)
                         assert)]
    (cast Event (doto (.execute insert-request)
                  assert))))
