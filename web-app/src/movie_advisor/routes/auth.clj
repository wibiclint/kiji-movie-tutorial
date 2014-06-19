(ns movie-advisor.routes.auth
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
  (redirect "/"))

(defn handle-logout []
  (session/clear!)
  (redirect "/"))

(defroutes auth-routes
  ; Login
  (GET "/login" [] (login-page))
  (POST "/login" [user] (handle-login user))

  ; Logout (this is post-only)
  (ANY "/logout" [] (handle-logout)))
