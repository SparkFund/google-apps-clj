(ns google-apps-clj.google-calendar
  (:require [clojure.core.typed :as t]
            [clojure.core.typed.unsafe :as tu]
            [clojure.edn :as edn :only [read-string]]
            [clojure.java.io :as io :only [as-url file resource]]
            [google-apps-clj.credentials :as cred])
  (:import (com.google.api.client.util DateTime)
           (com.google.api.services.calendar.model Event
                                                   EventAttendee
                                                   EventDateTime Events)
           (com.google.api.services.calendar Calendar
                                             Calendar$Builder
                                             Calendar$Events$Insert
                                             CalendarScopes)
           (com.google.api.client.googleapis.auth.oauth2 GoogleCredential)))

(t/ann build-calendar-service [cred/GoogleAuth -> Calendar])
(defn build-calendar-service
  "Given a google-ctx configuration map, builds a Calendar service using
  credentials coming from the OAuth2.0 credential setup inside googlectx"
  [google-ctx]
  (let [calendar-builder (->> google-ctx
                           cred/build-credential
                           (Calendar$Builder. cred/http-transport cred/json-factory))]
    (cast Calendar (doto (.build calendar-builder)
                     assert))))

(t/ann add-calendar-event [cred/GoogleAuth String String String String String (t/Coll String) Boolean -> Event])
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

(t/ann add-calendar-time-event [cred/GoogleAuth String String String String String (t/Coll String) -> Event])
(defn add-calendar-time-event
  "Given a google-ctx configuration map, a title, a description, a location,
   a start and end time (in YYYY-MM-DDTHH:MM:SS(+ or - hours off GMT like 4:00)),
   and a list of attendees email addresses, creates a calendar event by calling
   add-calendar-event as a time event"
  [google-ctx title description location start-time end-time attendees]
  (add-calendar-event google-ctx title description location start-time end-time attendees false))

(t/ann add-calendar-day-event [cred/GoogleAuth String String String String String (t/Coll String) -> Event])
(defn add-calendar-day-event
  "Given a google-ctx configuration map, a title, a description, a location,
   a start and end time (in YYYY-MM-DD), and a list of attendees email addresses,
   creates a calendar event by calling add-calendar-event as an all day event"
  [google-ctx title description location start-time end-time attendees]
  (add-calendar-event google-ctx title description location start-time end-time attendees true))

(t/ann list-events [cred/GoogleAuth String String -> (t/Seq Event)])
(defn list-events
  "Given a google-ctx configuration map, a start time and an end time
   (in YYYY-MM-DDTHH:MM:SS(+ or - hours off GMT like 4:00)),
   returns a list of this user's events for the given time period"
  [google-ctx start-time end-time]
  (let [calendar-service (build-calendar-service google-ctx)
        events (doto (.events ^Calendar calendar-service)
                 assert)
        start-time (DateTime. ^String start-time)
        end-time (DateTime. ^String end-time)
        list-events (doto (.list events "primary")
                      assert
                      (.setTimeMin start-time)
                      (.setTimeMax end-time)
                      (.setOrderBy "startTime")
                      (.setSingleEvents true))
        days-events (doto (.execute list-events)
                      assert)]
    (tu/ignore-with-unchecked-cast (doto (.getItems ^Events days-events)
                                     assert)
                                   (t/Seq Event))))

(t/ann list-day-events [cred/GoogleAuth String String -> (t/Seq Event)])
(defn list-day-events
  "Given a google-ctx configuration map, a day in the form(YYYY-MM-DD),
   and an offset from the GMT time zone(in the form + or - 04 or 11, etc),
   returns a list of this user's events for the given day"
  [google-ctx day time-zone-offset]
  (let [start-time (str day "T00:00:00" time-zone-offset ":00")
        end-time (str day "T23:59:59" time-zone-offset ":00")]
    (list-events google-ctx start-time end-time)))

(t/ann list-events-by-name [cred/GoogleAuth String Number -> (t/Seq Event)])
(defn list-events-by-name
  "Given a google-ctx configuration map, a title that will be the query
   of the event(this could be an email, title, description), and the max 
   amount of results you wish to receive back, finds all events with these
   specifications under the user's calendar"
  [google-ctx title max-results]
  (let [calendar-service (build-calendar-service google-ctx)
        events (doto (.events ^Calendar calendar-service)
                 assert)
        start-time (DateTime. ^java.util.Date (java.util.Date.))
        list-events (doto (.list events "primary")
                      assert
                      (.setQ title)
                      (.setMaxResults (int max-results))
                      (.setTimeMin start-time)
                      (.setOrderBy "startTime")
                      (.setSingleEvents true))
        days-events (doto (.execute list-events)
                      assert)]
    (tu/ignore-with-unchecked-cast (doto (.getItems ^Events days-events)
                                     assert)
                                   (t/Seq Event))))
