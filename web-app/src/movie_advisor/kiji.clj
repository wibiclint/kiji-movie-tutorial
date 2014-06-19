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

; Get a map from movie ids to ratings (MovieRating) for this user
; Values will be nil if the movie is not rated.
(defn get-movie-ratings
  [user-id movie-ids]
  ; TODO: Check that movie-id is an seq?
  (timbre/info "Getting movie ratings for user id " user-id " and movies " movie-ids)
  (let [table-reader (.openTableReader users-table)
        entity-id (get-entity-id users-table user-id)]
    ; For every movie ID, do a read in Kiji to get the movie information.
    (into {} (map 
      (fn [movie-id] 
        (let [data-request (KijiDataRequest/create "ratings" (str movie-id))
              rating-or-nil (-> (.get table-reader entity-id data-request) (.getMostRecentValue "ratings" (str movie-id)))]
          (timbre/info "Rating for movie " movie-id " is " rating-or-nil)
          [movie-id rating-or-nil])
      ) movie-ids))
  ))

; Connect to Kiji and read back user information.
(defn get-user-info
  [userid]
  (let [table-reader (.openTableReader users-table)
        entity-id (get-entity-id users-table userid)
        data-request (KijiDataRequest/create "info" "info")
        user-info (-> (.get table-reader entity-id data-request) (.getMostRecentValue "info" "info"))]
    ; "user-info" is of type "Person"
    user-info))

; Connect to Kiji and read back movie information.
(defn get-movie-info
  [movieid]
  (let [table-reader (.openTableReader movies-table)
        entity-id (get-entity-id movies-table movieid)
        data-request (KijiDataRequest/create "info" "info")
        movie-info (-> (.get table-reader entity-id data-request) (.getMostRecentValue "info" "info"))]
    ; "movie-info" is of type "MovieInfo"
    movie-info))


; Return vector of similar movies (sorted by similarity)
(defn get-most-similar-movies
  [movieid]
  (let [table-reader (.openTableReader movies-table)
        entity-id (get-entity-id movies-table movieid)
        data-request (KijiDataRequest/create "most_similar" "most_similar")
        most-similar (-> (.get table-reader entity-id data-request) (.getMostRecentValue "most_similar" "most_similar"))]
    ; TODO: Assert not null
    ; Turn into a list
    (.getSimilarities most-similar)))
