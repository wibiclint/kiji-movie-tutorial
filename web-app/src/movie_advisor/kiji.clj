(ns movie-advisor.kiji
  (:import (org.kiji.schema Kiji Kiji$Factory KijiURI KijiDataRequest)
           (org.kiji.scoring FreshKijiTableReader$Builder)))

(defn init []

  ; Connect to Kiji!
  (def kiji-uri (.build (KijiURI/newBuilder "kiji://localhost:2181/tutorial")))
  (def kiji (Kiji$Factory/open kiji-uri)))

; Connect to Kiji and read back the recommended movies!
; Return a list of show IDs, sorted from most- to least-highly recommended.
(defn get-top-N-movies-for-user
  [userid]
  (let [kiji-table (.openTable kiji "users")
        fresh-kiji-table-reader (-> (FreshKijiTableReader$Builder/create)
                                    (.withTable kiji-table)
                                    (.build))
        ; Calling a Java varargs method from Clojure requires passing in an array
        entity-id (.getEntityId kiji-table (to-array [(.toString userid)]))
        data-request (KijiDataRequest/create "recommendations" "foo")
        ; Get a MovieRecommendations object
        movie-recs (-> (.get fresh-kiji-table-reader entity-id data-request)
                       (.getMostRecentValue "recommendations" "foo"))]
    ; Now remove the 
    (map #(.getShowId %) (.getRecommendations movie-recs))))

; Connect to Kiji and read back user information.
; Return a list of show IDs, sorted from most- to least-highly recommended.
(defn get-user-info
  [userid]
  (let [kiji-table (.openTable kiji "users")
        table-reader (.openTableReader kiji-table)
        ; Calling a Java varargs method from Clojure requires passing in an array
        entity-id (.getEntityId kiji-table (to-array [(.toString userid)]))
        data-request (KijiDataRequest/create "info" "info")
        user-info (-> (.get table-reader entity-id data-request) (.getMostRecentValue "info" "info"))]
    ; "user-info" is of type "Person"
    user-info))
