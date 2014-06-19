(ns movie-advisor.routes.home
  (:use compojure.core)
  (:require [movie-advisor.layout :as layout]
            [movie-advisor.util :as util]
            [movie-advisor.kiji :as kiji]
            [hiccup.element :refer :all]
            [hiccup.form :refer :all]
            [noir.session :as session]
            [noir.response :refer [redirect]]
            [taoensso.timbre :as timbre]))

;--------------------------------------------------------------------------------
; Home page stuff

; No user logged in!
;
; Show random movies.
; Show globally most-popular movies?
; Prompt for login.
(defn home-page-no-user []
  (layout/common
    [:h1 "Home page"]
    [:p "No user logged in"]
    (link-to "/login" "log in!")
    [:p "Kiji URI is " (.toString (.getURI kiji/kiji))]))

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
      [:h1 "Home page" userid]
      [:p "User " userid " logged in"]
      [:p "User info = " (.toString user-info)]
      [:p "Top movies = " (apply str (interpose "," movie-recs))]
      (form-to [:post "/movies"]
               [:p "Enter the ID of a movie to view / rate:"]
               (text-field "movie-id")
               [:br]
               (submit-button "go"))

      (link-to "/logout" "log out"))))

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
    [:h1 "Login page!"]
    ; Text box with login
    (form-to [:post "/login"]
             [:p "User Id:"]
             (text-field "user")
             [:br]
             (submit-button "login"))
))

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

; Return html-formatted movies and ratings
(defn print-movie-ratings [movies-to-ratings]
  [:p "Most-similar movies and their ratings for this user!"]
  [:br]
  [:ul
  (map
    (fn [[movie-id rating]] [:li (if rating
                             (str "Movie " movie-id " got rating " rating)
                             (str "Movie " movie-id " was not rated!"))]) movies-to-ratings)
   ]
)

; Print out content if user *has* rated this movie.
; rating-info is of type MovieRating
(defn movie-page-if-user-has-rated [movie-id rating-info]
  [[:p "You gave this movie a rating of " (.getRating rating-info) " stars."]])

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
        ;_ (timbre/info "Got ratings " ratings)
        movies-to-similarities (into {} (map #(vector (.getItem %) (.getSimilarity %)) most-similar))
        ;_ (timbre/info "Got movies to similarities " movies-to-similarities)
        avg-num (reduce + (map (fn [[movieid rating]] (* (.getRating rating) (get movies-to-similarities movieid))) ratings))
        ;_ (timbre/info "Got numerator")
        avg-den (reduce + (map (fn [[movieid rating]] (get movies-to-similarities movieid)) ratings))]
        ;_ (timbre/info "Got denom")
    [[:p "Predicted rating for this movie = " (/ (* 1.0 avg-num) avg-den)]]
))


; Render the page for a movie id!
(defn movie-page [movie-id]
  ; Define the initial content
  (let
    [user-id (session/get :user)
     standard-content [[:h1 "Movie page!"] [:p (str "Movie ID = " movie-id)] [:p (kiji/get-movie-info movie-id)]]
     movie-ratings (kiji/get-movie-ratings user-id [movie-id])
     additional-content (if (nil? (get movie-ratings movie-id))
                                  (movie-page-if-user-has-not-rated user-id movie-id)
                                  (movie-page-if-user-has-rated movie-id (get movie-ratings movie-id)))]
    ;(timbre/info "Let's render the page!")
  (layout/common (concat standard-content additional-content)))

    ; Get either:
    ; - the user's rating for this movie and similar movies he/she might like
    ; - a predicted rating for this movie
)

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
  (GET "/movies/:movie-id" [movie-id] (movie-page movie-id))
  ;(POST "/movies/:movie-id" [rating] (rate-movie rating))

  ; Home page
  (GET "/" [] (home-page))
)
