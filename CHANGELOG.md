# google-apps-clj changes

0.6.1 (2017-01-18)
------------------

**Bug Fixes**

* Fix write-sheets with one row
* Fix reading empty currency and date cells

0.6.0 (2017-01-12)
------------------

**Features**

* Substantially revises google-apps-clj.google-sheets-v4 vars

0.5.3 (2016-08-14)
------------------

**Features**

* Add support for Google Sheets v4 api

0.5.2 (2016-06-06)
------------------

**Bug Fixes**
* [#19](https://github.com/SparkFund/google-apps-clj/issues/19): in `project.clj`: Add useless Jetty
library to `:exclusions` to help prevent conflicts in downstream consumers


0.5.1 (2016-03-14)
------------------

**Bug Fixes**
* [#18](https://github.com/SparkFund/google-apps-clj/issues/18): in Credentials: Tweak type definition for
`GoogleCtx` property `:redirect-uris` from `t/ISeq` to the more general `t/Seqable`


0.5.0 (2016-03-11)
------------------

:warning: This release includes major breaking changes in the Google Drive code.

**Breaking Changes**

* Drive: *Major change*: Complete overhaul of `google-drive` namespace to simplify and organize the code, and to make
the type checker pass.  Many type-defs and functions have been renamed, or simplified to take fewer unnamed arguments
and instead take a map with additional options (for example, see `upload-file!`) which should make it easier to use.
Two protocols used in converting Objects into usable data (`Requestable` and `Response`) have been replaced with
simpler functions instead.
* Drive: Map keys for literal `true` values will no longer automatically be appended with a `?`.  Before this change,
we would convert `{"canInvite" true}` to `{:can-invite? true}`.  After this change, it will instead be transformed
to `{:can-invite true}` (note the lack of a question mark).  Keys for other values will not be changed
(e.g. `{"canInvite" false, "name" "test"}` still becomes `{:can-invite false, :name "test"}`

**Features**

* Type Checking: All namespaces now pass `core.typed` type checking via `lein typed check`.  Type checking helped
uncover and fix multiple bugs in the existing code.
* CI: Type checking will run automatically for commits in this repo; this should help keep type annotations from
becoming so broken


0.4.4 (2016-03-07)
------------------

**Features**

* Drive: Allow opt-in uploading of files via [Direct Uploading](https://developers.google.com/api-client-library/java/google-api-java-client/media-upload#direct)
which is much faster for small files (although Direct Uploads are not resumable).  See https://github.com/SparkFund/google-apps-clj/issues/15


0.4.3 (2016-02-29)
------------------

**Bug Fixes**

* Fixed some (but not all) type checking failures in `google-sheets` and `google-drive` namespaces
* Updated `build-credential-from-ctx` to return a `GoogleCredential` instance, which plays nicer with GSheets APIs.
Since `GoogleCredential` implements `HttpRequestInitializer`, client code shouldn't require modification
(although you may have to change type annotations if you referenced `HttpRequestInitializer`)


0.4.2 (2016-02-29)
------------------

**Features**

* Allow specifying default credential location using a System Property called `GOOGLE_APPLICATION_CREDENTIALS`
(this previously could be done with an environment variable of the same name; the system property will take precedence,
and if neither a system property nor an environment variable is specified, we'll still fall back to the Google
client library's default behavior, which also searches a predetermined file location)

**Documentation**

* Added documentation in [doc/RELEASING.md](doc/RELEASING.md) covering steps needed to cut a new release of this library


0.4.1 (2016-02-26)
------------------

**Bug Fixes**

* Fixed two slight dependency mismatches between `com.google.gdata/core` and other
Google dependencies, by adding exclusions to `com.google.gdata/core`:
  - `com.google.code.findbugs/jsr305` is `1.3.7` in `gdata/core`, `1.3.9` elsewhere
  - `org.apache.httpcomponents/httpclient` is `4.0.1` in `gdata/core`, `4.0.3` elsewhere


0.4.0 (2016-02-25)
------------------

**Breaking Changes**

* Many functions which previously were annotated to take a `credentials/GoogleCtx` map,
can now also accept a `GoogleCredential`. This changes the annotation to `credentials/GoogleAuth`.

**Features**

* Add support for [Service Account credentials](https://developers.google.com/identity/protocols/OAuth2ServiceAccount)
* Add helper methods for loading credentials, including [application default credentials](https://developers.google.com/identity/protocols/application-default-credentials)
* Add option to `google-drive/upload!` to prevent automatic conversion to Google Docs
* Improvements to `core.typed` annotations (still not complete)

**Documentation**

* Better changelog format
* Improved documentation to match new Google Developer Console
* Added instructions on how to obtain and use Service Account Credentials


0.3.3 (2016-01-14)
----------------------------------------

**Bug Fixes**

* Fix GoogleCtx type


0.3.2 (2016-01-13)
----------------------------------------

**Breaking Changes**

* Rename resolve-file-id! to find-file!, change semantics


0.3.1 (2016-01-08)
----------------------------------------

**Bug Fixes**

* Downgrade core.typed to 0.3.14


0.3.0 (2016-01-08)
----------------------------------------

**Features**

* Began tracking changes

**Breaking Changes**

* Major breaking changes to the drive ns, reworking it around
  queries as data structures. This allows them to be modified
  independently of their execution context (e.g. specifying fields)
  and also faciliates the single and batched execution contexts.




--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

### :warning: **WARNING** :warning:

**Changelog from 0.2.2 and earlier is reconstructed and may not be 100% accurate!**

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------


0.2.2 (2015-11-16)
----------------------------------------

**Features**

* In `google-drive`: Add `get-file-ids` to enumerate id:title map of all files
* In `google-drive`: Add `get-file` to fetch a File based on its ID


0.2.1 (2015-06-25)
----------------------------------------

**Bug Fixes**

* In `google-sheets`: `write-worksheet` resizes directly to the size we need
in order to avoid breaking frozen rows/columns


0.2.0 (2015-06-23)
----------------------------------------

**Features**

* New namespace `google-calendar`


0.1.3 (2015-06-22)
----------------------------------------

**Features**

* Many additions to `google-drive`


0.1.2 (2015-06-19)
----------------------------------------

**Misc**

* Transferred repo into SparkFund org
* Updated license from EPL to Apache 2.0


0.1.1 (2015-06-16)
----------------------------------------

**Features**

* Initial release
