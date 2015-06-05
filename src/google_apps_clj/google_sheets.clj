(ns google-apps-clj.google-sheets
  (:require)
  (:import
   (com.google.gdata.data.spreadsheet SpreadsheetFeed
                                      WorksheetEntry
                                      ListFeed
                                      CellFeed
                                      ListEntry
                                      CellEntry
                                      WorksheetFeed)
   (com.google.gdata.data PlainTextConstruct
                          ILink$Rel
                          ILink$Type)
   (com.google.gdata.client.spreadsheet SpreadsheetService
                                        CellQuery
                                        WorksheetQuery
                                        SpreadsheetQuery)
   (com.google.gdata.data.batch BatchOperationType
                                BatchUtils)))
