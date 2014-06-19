(ns movie-advisor.routes.home
  (:use compojure.core)
  (:require [movie-advisor.layout :as layout]
            [movie-advisor.util :as util]
            [movie-advisor.kiji :as kiji]
            [hiccup.element :refer :all]
            [hiccup.form :refer :all]
            [noir.session :as session]
            [noir.response :refer [redirect]]
            [taoensso.timbre :as timbre])
  (:import (java.text SimpleDateFormat)
           (java.util Date)))

;--------------------------------------------------------------------------------
; Common stuff
(defn render-movie-selector []
  (form-to 
    {:class "pure-form"}
    [:post "/movies"]
    [:legend "Enter the ID of a movie to view / rate"]
    (text-field {:size 5 :placeholder "Movie ID"} "movie-id") " "
    (submit-button {:class "pure-button"} "go")))

(defn long-to-date-string [date-as-long]
  (let [date-format (SimpleDateFormat. "MMM yyyy")
        date (Date. date-as-long)]
    (.toString (.format date-format date))))

; Render MovieInfo on the page.
(defn render-movie [movie-info]
  [:div.movie (link-to (str "/movies/" (.getMovieId movie-info)) (.getTitle movie-info))
    ;(str " - (" (long-to-date-string (.getTheaterReleaseDate movie-info)) ") ")
   ;" "
    ;(link-to (.getImdbUrl movie-info) "[IMDB]")
   ]
)

(defn movie-rating-form [movie-id message & extra-rating-options]
  (let [rating-options (concat [5 4 3 2 1] extra-rating-options)]
   (form-to 
     {:class "pure-form"}
     [:post (str "/movies/" movie-id)]
     [:legend message]
     (drop-down :rating rating-options 5) " "
     (submit-button {:class "pure-button"} "rate!"))))

;--------------------------------------------------------------------------------
; Home page stuff

; No user logged in!
;
; Show random movies.
; Show globally most-popular movies?
; Prompt for login.
(defn home-page-no-user []
  (layout/common
    [:h2 "Home page"]
    [:p "No user logged in"]))

; User is logged in!
;
; Show the top-N movies for this user.
; Provide a link to view / rate a movie.
; Provide a way to log out.
(defn home-page-user []
  (let [userid (session/get :user)
        user-info (kiji/get-user-info userid)
        movie-recs (kiji/get-top-N-movies-for-user-as-movie-info userid)]
    (layout/common
      ; TODO: Handle login for user that does not exist.
      [:h2 "Home page"]
      [:p "Welcome, user #" userid "!"]
      ;[:p "User info = " (.toString user-info)]
      [:h3 "Movies most recommended for you:"]
      (for [movie-info movie-recs] (render-movie movie-info))
      [:br]
      (render-movie-selector))))

(defn home-page []
  (if (nil? (session/get :user))
    ; No one is logged in!
    (home-page-no-user)
    ; Someone is logged in!
    (home-page-user)))

;--------------------------------------------------------------------------------
; Authorization stuff

(defn login-page []
  (layout/common
    [:h2 "Login page!"]
    ; Text box with login
    (form-to
      {:class "pure-form"}
      [:post "/login"]
      [:legend "Login information"] (text-field {:size 5 :placeholder "user id"} "user") " " (submit-button {:class "pure-button"} "login"))))

(defn handle-login [user]
  ; TODO: Check no current user!
  ; Store this user in the session
  (session/put! :user user)
  (redirect "/")
)

(defn handle-logout []
  (session/clear!)
  (redirect "/")
)

;--------------------------------------------------------------------------------
; View / rate movies

; Print out content if user *has* rated this movie.
; rating-info is of type MovieRating
(defn movie-page-if-user-has-rated [movie-id rating-info]
  [[:p "You gave this movie a rating of " (.getRating rating-info) " stars."]
   (movie-rating-form movie-id "Re-rate this movie!" ["unrate"])])

; Print out content if the user has *not* rated this movie.
; Estimate his/her rating by doing the following:
; - Get the most-similar movies to this movie
; - Filter out all of the movies that the user has not seen
; - Take the weighted average of the user's ratings for the similar movies that he has seen
(defn movie-page-if-user-has-not-rated [user-id movie-id]
  (let [most-similar (kiji/get-most-similar-movies movie-id)
        ratings-and-unseen (kiji/get-movie-ratings user-id (map #(.getItem %) most-similar))
        ; This will be a list of [movieid, rating] pairs without any nil ratings
        ratings (remove #(nil? (second %)) ratings-and-unseen)
        ; Create a map of movie to similarity
        movies-to-similarities (into {} (map #(vector (.getItem %) (.getSimilarity %)) most-similar))
        ; Compute the estimated rating for the user.
        avg-num (reduce + (map (fn [[movieid rating]] (* (.getRating rating) (get movies-to-similarities movieid))) ratings))
        avg-den (reduce + (map (fn [[movieid rating]] (get movies-to-similarities movieid)) ratings))
        predicted-rating (/ (* 1.0 avg-num) avg-den)
        message (cond
                  (> predicted-rating 4.0) "We think you will love this movie!"
                  (> predicted-rating 2.0) "You may like this movie."
                  :else "You may not like this movie.")]
    [[:p message] [:p (format "(Predicted rating = %.1f)" predicted-rating)]
     (movie-rating-form movie-id "Seen this movie?  Rate it now!")]
))


; Render the page for a movie id!
(defn movie-page [movie-id]
  ; Define the initial content
  (let
    [user-id (session/get :user)
     movie-info (kiji/get-movie-info movie-id)
     standard-content [ [:h2 (.getTitle movie-info)] [:p (link-to (.getImdbUrl movie-info) "View on IMDB")]]
     movie-ratings (kiji/get-movie-ratings user-id [movie-id])
     ; Get either:
     ; - the user's rating for this movie and similar movies he/she might like
     ; - a predicted rating for this movie
     additional-content (if (nil? (get movie-ratings movie-id))
                                  (movie-page-if-user-has-not-rated user-id movie-id)
                                  (movie-page-if-user-has-rated movie-id (get movie-ratings movie-id)))]
    ;(timbre/info "Let's render the page!")
  (layout/common (concat standard-content additional-content)))

)

(defn rate-movie [movie-id rating]
  (let [user-id (session/get :user)]
    (if (= rating "unrate")
      (kiji/delete-rating user-id movie-id)
      (kiji/rate-movie user-id movie-id rating))
    (redirect (str "/movies/" movie-id))))

(defroutes home-routes
  ; Login
  (GET "/login" [] (login-page))
  (POST "/login" [user] (handle-login user))

  ; Logout (this is post-only)
  (ANY "/logout" [] (handle-logout))

  ; Choose a movie to look at
  ; TODO: Check movie-id is legal somehow
  (POST "/movies" [movie-id] (redirect (str "/movies/" movie-id)))

  ; Movie page
  (GET  ["/movies/:movie-id", :movie-id #"[0-9]+"] [movie-id] (movie-page movie-id))
  (POST "/movies/:movie-id" [movie-id rating] (rate-movie movie-id rating))

  ; Home page
  (GET "/" [] (home-page))
)
