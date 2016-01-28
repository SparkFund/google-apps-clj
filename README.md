# google-apps-clj

A Clojure library that wraps the Google Java API for different Google
Applications. This library is partially typed using core.typed.

## Obtaining

```
[google-apps-clj "0.3.1"]
```

## Currently supported APIs

* Google OAuth 2.0
* Google Drive
* Google Spreadsheets
* Google Calendar

## Usage
In order to use any of these APIs, you must first use the Google OAuth 2.0 library to set up your credentials. All the APIs rely on the credentials received from setting up OAuth 2.0.

### Obtaining OAuth 2 Credentials

1. Create a new OAuth credential 
    1. Navigate to the [Developer's Console](https://console.developers.google.com)
    2. Create a project and name it appropriately
    3. Navigate to `Dashboard > API > Credentials` 
    4. Click *New Credentials* > *OAuth Client ID*. Choose "Other" 
    5. Note *Client ID* and *Client Secret* 
1. Get and store credentials locally 
    1. Copy the template [google-creds.edn file](https://github.com/dunn-mat/google-apps-clj/blob/master/config/google-creds.edn.template)
    1. Put your *Client ID* and *Client Secret* into `google-creds.edn`. You will use the data in this file for getting the rest of your credententials and for the other APIs.
    1. Generate `auth_map` 
    ```clojure
   ;(def scopes  ["https://www.googleapis.com/auth/drive" "https://docs.google.com/feeds/" "https://spreadsheets.google.com/feeds" "https://www.googleapis.com/auth/calendar"] )
   (def scopes ["https://www.googleapis.com/auth/calendar"])
   (-> "google-cres.edn" 
       slurp 
       clojure.edn/read-string 
       (google-apps-clj.credentials/get-auth-map scopes))
    ```
    1. Copy the returned data into your `google-creds.edn` file under the `:auth-map` key. N.B. keys should _not_ be quoted and should contain `-` not `_`. 

1. Test your configuration
     ```clojure
     ; load credentials now with auth_map completed
     (def google-ctx 
       (-> "google-creds.edn" 
       slurp 
       clojure.edn/read-string ))
      ; add an event to the calendar
      (def gev 
         (google-apps-clj.google-calendar/add-calendar-time-event 
           google-ctx "title" "desc" "loc" 
           "2016-01-27T12:00:00" "2016-01-27T14:00:00" [] ))
      ```
 

### Drive API

##### Supported Functionality

* Searching
* Fetching
* Uploading
* Updating
* Deleting
* Authorizing

### Spreadsheet API

##### Supported Functionality

* Creating a Worksheet inside of an existing Spreadsheet
* Finding Spreadsheets by title and id, Worksheets by title and id, Cells by row column notation
* Editing a Worksheet's rows, columns, and title
* Editing individual cells in a Worksheet
* Editing rows in a worksheet
* Batch-updating large quantity of cells (improves performance)
* Reading an entire Worksheet's values or headers, or both
* Overwriting an entire Worksheet with a new set of data (destroys old data)

### Calendar API

##### Supported Functionality

* Listing all events within a certain date-time range (for user)
* Listing all events on a given day (for user)
* Listing upcoming events with a supplied name (for user)
* Creating an event with a certain time range
* Creating an all day event
* Deleting event by ID

## What's Next?

* Retrieve single event by ID
* Update event by ID

#### General

* Allow service account authentication in addition to the current
  user-account OAuth funkiness
* Consider ditching the baroque Google java library in favor of
  direct integration with the api using clj-http or the like
* Mitigate "WARNING: Application name is not set. Call Builder#setApplicationName."

#### Drive API
* Consider making file maps the unit of work for the command fns
* Or possibly an Identifiable protocol to allow file ids or maps

#### Sheets API
* Consider a cleaner abstraction instead of a grab bag of fns

#### Calendar API

## License

Copyright © SparkFund 2015-2016

Distributed under the Apache License, Version 2.0.
