# google-apps-clj

A Clojure library that wraps the Google Java API for different Google Applications.

## Currently supported APIs

* Google OAuth 2.0
* Google Drive
* Google Spreadsheets.

## Usage
In order to use any of these APIs, you must first use the Google OAuth 2.0 library to set up your credentials. All the APIs rely on the credentials received from setting up OAuth 2.0.

### Obtaining OAuth 2 Credentials

1. Log in to Google using the account you wish the credentials to be under
2. Navigate to the Developer's Console (https://console.developers.google.com)
3. Create a project and name it appropriately
4. Navigate to Credentials and click *Create new Client ID* under OAuth
5. Create a google-creds.edn file (https://github.com/dunn-mat/google-apps-clj/blob/master/config/google-creds.edn.template)
6. Copy the Client ID, Client Secret, and Redirect URIs into your google-creds.edn. You will use the data in this file for getting the rest of your credententials and for the other APIs.
7. Read in your google-creds.edn file like so `(edn/read-string (slurp "config/dev/matt-google-creds.edn"))`
8. Call get-auth-map on this read in data and follow its instructions
9. Copy the returned data into your google-creds.edn file under the `:auth-map` key. Reload it into your REPL.
10. You are now ready to use the other APIs with your credential file

## License

Copyright © 2015 

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
