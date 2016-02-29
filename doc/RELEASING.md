# Releasing

To cut a new release of `google-apps-clj`, do the following:

* Update version number in `project.clj` and `README.md`
* Update `CHANGELOG.md` to include info about the changes (follow existing format; newest release on top)
* Commit this to `develop` branch with commit message `[Release] X.Y.Z` (e.g. `[Release] 0.4.1`)
* Merge the `develop` branch into `master` (this should be a fast-forward merge)
* Add a tag to the [Release] commit, tag name should be `vX.Y.Z` (e.g. `v0.4.1`)
* Push your commit and tag up to GitHub
* Deploy to Clojars by running `lein deploy clojars`
