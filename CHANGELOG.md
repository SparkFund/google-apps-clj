# google-apps-clj changes

## 0.3.3

* Fix GoogleCtx type

## 0.3.2

* Rename resolve-file-id! to find-file!, change semantics

## 0.3.1

* Downgrade core.typed to 0.3.14

## 0.3.0

* Began tracking changes
* Major breaking changes to the drive ns, reworking it around
  queries as data structures. This allows them to be modified
  independently of their execution context (e.g. specifying fields)
  and also faciliates the single and batched execution contexts.
