(ns movie-advisor.routes.home
  (:use compojure.core)
  (:require [movie-advisor.layout :as layout]
            [movie-advisor.util :as util]
            [movie-advisor.kiji :as kiji]
            [hiccup.element :refer :all]
            [hiccup.form :refer :all]
            [noir.session :as session]
            [noir.response :refer [redirect]]
            ))

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
        movie-recs (kiji/get-top-N-movies-for-user userid)
        ]
    (layout/common
      [:h1 "Home page" userid]
      [:p "User " userid " logged in"]
      [:p "User info = " (.toString user-info)]
      [:p "Top movies = " (apply str (interpose "," movie-recs))]
      (link-to "/logout" "log out"))))

(defn home-page []
  (if (nil? (session/get :user))
    ; No one is logged in!
    (home-page-no-user)

    ; Someone is logged in!
    (home-page-user)))

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

; Render the page for a movie id!
(defn movie-page [movie-id]
  )


(defroutes home-routes
  ; Login
  (GET "/login" [] (login-page))
  (POST "/login" [user] (handle-login user))

  ; Logout (this is post-only)
  (ANY "/logout" [] (handle-logout))

  ; Movie page
  (GET "/movies/:move-id" [movie-id] (movie-page))
  ;(POST "/movies/:move-id" [rating] (rate-movie rating))

  ; Home page
  (GET "/" [] (home-page))
)
