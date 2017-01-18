# google-apps-clj

A Clojure library that wraps the Google Java API for different Google Applications.

This library is partially typed using `core.typed`.  Run `lein typed check` to type-check the library's code.

Latest test results (Thanks, [CircleCI](https://github.com/CircleCI)!):

* `develop`: [![Circle CI](https://circleci.com/gh/SparkFund/google-apps-clj/tree/develop.svg?style=svg)](https://circleci.com/gh/SparkFund/google-apps-clj/tree/develop)
* `master`: [![Circle CI](https://circleci.com/gh/SparkFund/google-apps-clj/tree/master.svg?style=svg)](https://circleci.com/gh/SparkFund/google-apps-clj/tree/master)


## Installing

If you're using Leiningen, just add this to your `project.clj`:

```clj
[google-apps-clj "0.6.1"]
```

If you are using ClojureScript, there's a dependency conflict (see [#22](https://github.com/SparkFund/google-apps-clj/issues/22)) between Google Closure Compiler and the Google Apps API library , so you'll have to add an exclusion, like so:

```clj
[google-apps-clj "0.6.1" :exclusions [com.google.guava/guava-jdk5]]
```

Check out [CHANGELOG.md](CHANGELOG.md) to see what's been updated lately.

#### Upgrading from 0.5.3

The sheets v4 ns was substantially revised.

#### Upgrading from 0.4.x

Major breaking changes were made to the Google Drive code in v0.5.0 which will require updating most of
your code that interacts with Google Drive.  Please see [CHANGELOG.md](CHANGELOG.md) for details.

## Currently supported APIs

* Google OAuth 2.0
* Google Drive
* Google Spreadsheets (v3 and v4)
* Google Calendar

## Usage
In order to use any of these APIs, you must first use the Google OAuth 2.0 library to set up your credentials. All the APIs rely on the credentials received from setting up OAuth 2.0.

### Obtaining OAuth 2 Credentials

1. Log in to Google using the account you wish the credentials to be under
2. Navigate to the [Developer's Console](https://console.developers.google.com)
3. Create a project and name it appropriately
4. On the project dashboard, find the "Use Google APIs" card and click the link to "Enable and manage APIs".  On the left nav bar, select the credentials tab.  You can also go directly to https://console.developers.google.com/apis/credentials?project=your-project-id (substituting in for `your-project-id`, of course).
5. Navigate to Credentials and click *Create new Client ID* under OAuth. Choose "Installed application" and set up a consent screen if necessary.
6. Create a [google-creds.edn file](config/google-creds.edn.template)
7. Copy the Client ID, Client Secret, and Redirect URIs into your google-creds.edn. You will use the data in this file for getting the rest of your credententials and for the other APIs.
8. Read in your google-creds.edn file like so:

     `(edn/read-string (slurp "config/google-creds.edn"))`

9. Call google-apps-clj.credentials/get-auth-map on this, along with the necessary [OAuth scopes](https://developers.google.com/identity/protocols/googlescopes) (eg `["https://www.googleapis.com/auth/drive" "https://docs.google.com/feeds/" "https://spreadsheets.google.com/feeds" "https://www.googleapis.com/auth/calendar"]`), and read in data and follow its instructions
10. Copy the returned data into your google-creds.edn file under the `:auth-map` key. Reload it into your REPL.
11. You are now ready to use the other APIs with your credential file

### Using Service Account Credentials

Google also supports [Service Account credentials](https://developers.google.com/identity/protocols/OAuth2ServiceAccount)
for server-to-server API access.  The credential is provided as a JSON file containing a private key and some other data.

To obtain a service credential:

1. Log in to Google using the account you wish the credentials to be under
2. Navigate to the [Developer's Console](https://console.developers.google.com)
3. Create a project and name it appropriately
4. Navigate to the [API Manager](https://console.developers.google.com/apis/library), and enable the APIs you need an disable the default-provided ones you don't need
5. Navigate to the [_Permissions > Service Accounts_ page](https://console.developers.google.com/permissions/serviceaccounts)
6. Create a new service account, selecting the option to obtain a new private key as JSON.
7. You should be given a JSON file containing the credential.  You have a few options now:
  * Store it anywhere, and set an environment variable (`GOOGLE_APPLICATION_CREDENTIALS`) to point to it
  * On Mac/Linux, rename and move the JSON file to be `~/.config/gcloud/application_default_credentials.json` (making directories as needed)
  * On Windows, rename and move the JSON file to be `%APPDATA%\gcloud\application_default_credentials.json` (making directories as needed)

Now you can use `google-apps-clj.credentials/default-credential` to load these credentials, and then
pass them into most places that might otherwise expect a `google-ctx`.  For example:

```clj
(let [scopes [com.google.api.services.drive.DriveScopes/DRIVE]
      creds (google-apps-clj.credentials/default-credential scopes)]
    (google-apps-clj.google-drive/list-files! creds "FOLDERIDFOLDERIDFOLDERIDFOLD"))
```

(note in this scenario that the service account user has to be given the appropriate permissions in Drive.
The service account user won't show up by name in searches, but it can be added as a viewer/editor using its
`USERID@PROJECTID.iam.gserviceaccount.com` email address as found in the JSON credential file)


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

* Consider ditching the baroque Google java library in favor of
  direct integration with the api using clj-http or the like

#### Drive API
* Consider making file maps the unit of work for the command fns
* Or possibly an Identifiable protocol to allow file ids or maps

#### Sheets API
* Consider a cleaner abstraction instead of a grab bag of fns

#### Calendar API

## License

Copyright © SparkFund 2015-2017

Distributed under the Apache License, Version 2.0. See [LICENSE.txt](LICENSE.txt) for details.
