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

(t/ann add-calendar-event [cred/GoogleCtx String String String String String (t/Coll String) Boolean -> Event])
(defn- add-calendar-event
  "Given a google-ctx configuration map, a title, a description, a location, a start and
   end time (in either YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS(+ or - hours off GMT like 4:00)),
   a list of attendees email addresses, and whether this event is in day format or time format,
   creates an event on the user's calendar with the above specifications"
  [google-ctx title description location start-time end-time attendees all-day?]
  (let [calendar-service (build-calendar-service google-ctx)
        events (doto (.events ^ Calendar calendar-service)
                 assert)
        start-time (DateTime. ^String start-time)
        start (if all-day?
                (doto (EventDateTime.)
                  (.setDate start-time))
                (doto (EventDateTime.)
                  (.setDateTime start-time)))
        end-time (DateTime. ^String end-time)
        end (if all-day?
                (doto (EventDateTime.)
                  (.setDate end-time))
                (doto (EventDateTime.)
                  (.setDateTime end-time)))
        attendees (tu/ignore-with-unchecked-cast
                   (map #(doto (EventAttendee.)
                           (.setEmail %)) attendees)
                   (java.util.List EventAttendee))
        event (doto (Event.)
                (.setSummary title)
                (.setDescription description)
                (.setLocation location)
                (.setAttendees attendees)
                (.setStart start)
                (.setEnd end))
        insert-request (doto (.insert events "primary" event)
                         assert)]
    (cast Event (doto (.execute insert-request)
                  assert))))

(t/ann add-calendar-time-event [cred/GoogleCtx String String String String String (t/Coll String) -> Event])
(defn add-calendar-time-event
  "Given a google-ctx configuration map, a title, a description, a location,
   a start and end time (in YYYY-MM-DDTHH:MM:SS(+ or - hours off GMT like 4:00)),
   and a list of attendees email addresses, creates a calendar event by calling
   add-calendar-event as a time event"
  [google-ctx title description location start-time end-time attendees]
  (add-calendar-event google-ctx title description location start-time end-time attendees false))

(t/ann add-calendar-day-event [cred/GoogleCtx String String String String String (t/Coll String) -> Event])
(defn add-calendar-day-event
  "Given a google-ctx configuration map, a title, a description, a location,
   a start and end time (in YYYY-MM-DD), and a list of attendees email addresses,
   creates a calendar event by calling add-calendar-event as an all day event"
  [google-ctx title description location start-time end-time attendees]
  (add-calendar-event google-ctx title description location start-time end-time attendees true))
