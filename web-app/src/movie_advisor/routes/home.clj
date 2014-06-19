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
  [:div.movie (link-to (str "/movies/" (.getMovieId movie-info)) (.getTitle movie-info))]
)


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



(defroutes home-routes
  ; Home page
  (GET "/" [] (home-page))
)
