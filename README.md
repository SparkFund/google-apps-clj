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

1. Log in to Google using the account you wish the credentials to be under
2. Navigate to the [Developer's Console](https://console.developers.google.com)
3. Create a project and name it appropriately
4. Navigate to Credentials and click *Create new Client ID* under OAuth. Choose "Installed application" and set up a consent screen if necessary.
5. Create a [google-creds.edn file](https://github.com/dunn-mat/google-apps-clj/blob/master/config/google-creds.edn.template)
6. Copy the Client ID, Client Secret, and Redirect URIs into your google-creds.edn. You will use the data in this file for getting the rest of your credententials and for the other APIs.
7. Read in your google-creds.edn file like so:

     `(edn/read-string (slurp "config/matt-google-creds.edn"))`

8. Call google-apps-clj.credentials/get-auth-map on this, along with the necessary scopes (eg `["https://www.googleapis.com/auth/drive" "https://docs.google.com/feeds/" "https://spreadsheets.google.com/feeds" "https://www.googleapis.com/auth/calendar"]`), and read in data and follow its instructions
9. Copy the returned data into your google-creds.edn file under the `:auth-map` key. Reload it into your REPL.
10. You are now ready to use the other APIs with your credential file

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

## What's Next?

#### General

* Allow service account authentication in addition to the current
  user-account OAuth funkiness
* Consider ditching the baroque Google java library in favor of
  direct integration with the api using clj-http or the like

#### Drive API
* Consider making file maps the unit of work for the command fns
* Or possibly an Identifiable protocol to allow file ids or maps

#### Sheets API
* Consider a cleaner abstraction instead of a grab bag of fns

#### Calendar API

## License

Copyright © SparkFund 2015-2016

Distributed under the Apache License, Version 2.0.
