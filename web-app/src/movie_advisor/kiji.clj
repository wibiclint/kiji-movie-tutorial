(ns movie-advisor.kiji
  (:require [taoensso.timbre :as timbre])
  (:import (org.kiji.schema Kiji Kiji$Factory KijiURI KijiDataRequest)
           (org.kiji.scoring FreshKijiTableReader$Builder)))

(defn init []

  ; Connect to Kiji!
  (def kiji-uri (.build (KijiURI/newBuilder "kiji://localhost:2181/tutorial")))
  (def kiji (Kiji$Factory/open kiji-uri))
  (def users-table (.openTable kiji "users"))
  (def movies-table (.openTable kiji "movies")))

(defn get-entity-id [kiji-table movie-or-user-id]
  (.getEntityId kiji-table (to-array [(.toString movie-or-user-id)])))

; Connect to Kiji and read back the recommended movies!
; Return a list of show IDs, sorted from most- to least-highly recommended.
(defn get-top-N-movies-for-user-as-id
  [userid]
  (let [fresh-kiji-table-reader (-> (FreshKijiTableReader$Builder/create)
                                    (.withTable users-table)
                                    ; Make this a really huge timeout!
                                    (.withTimeout 5000)
                                    (.build))
        ; Calling a Java varargs method from Clojure requires passing in an array
        entity-id (get-entity-id users-table userid)
        data-request (KijiDataRequest/create "recommendations" "foo")
        ; Get a MovieRecommendations object
        kiji-result (.get fresh-kiji-table-reader entity-id data-request)
        movie-recs (.getMostRecentValue kiji-result "recommendations" "foo")]
    ; For now, get only the show IDs
    (timbre/info "User id is " userid)
    (timbre/info "Entity ID is " entity-id)
    (timbre/info "Finished getting most recent value back from FreshKijiTableReader")
    (timbre/info "Kiji result is " kiji-result)
    (timbre/info "Got back value " movie-recs)
    (map #(.getShowId %) (.getRecommendations movie-recs))))

; Get the top N movies for a user, including all information about the movie
(defn get-top-N-movies-for-user-as-movie-info
  [userid]
  (let [movie-ids (get-top-N-movies-for-user-as-id userid)
        table-reader (.openTableReader movies-table)]
    ; For every movie ID, do a read in Kiji to get the movie information.
    (map 
      (fn [movie-id] 
        (let [entity-id (get-entity-id movies-table movie-id)
              data-request (KijiDataRequest/create "info" "info")]
          (-> (.get table-reader entity-id data-request) (.getMostRecentValue "info" "info"))))
      movie-ids)))


; Connect to Kiji and read back user information.
; Return a list of show IDs, sorted from most- to least-highly recommended.
(defn get-user-info
  [userid]
  (let [table-reader (.openTableReader users-table)
        ; Calling a Java varargs method from Clojure requires passing in an array
        entity-id (.getEntityId users-table (to-array [(.toString userid)]))
        data-request (KijiDataRequest/create "info" "info")
        user-info (-> (.get table-reader entity-id data-request) (.getMostRecentValue "info" "info"))]
    ; "user-info" is of type "Person"
    user-info))
