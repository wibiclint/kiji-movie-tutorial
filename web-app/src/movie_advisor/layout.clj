(ns movie-advisor.layout
  (:require [selmer.parser :as parser]
            [clojure.string :as s]
            [hiccup.page  :refer [html5 include-css]]
            [hiccup.element :refer [link-to]]
            [noir.session :as session]
            [ring.util.response :refer [content-type response]]
            [compojure.response :refer [Renderable]]))

(def template-path "templates/")

(deftype RenderableTemplate [template params]
  Renderable
  (render [this request]
    (content-type
      (->> (assoc params
                  (keyword (s/replace template #".html" "-selected")) "active"
                  :servlet-context (:context request))
        (parser/render-file (str template-path template))
        response)
      "text/html; charset=utf-8")))

(defn render [template & [params]]
  (RenderableTemplate. template params))

; Render basic HTML
(defn common [& body]
  (let [login-or-out (if-let [user-id (session/get :user)]
                       (link-to "/logout" "Log out")
                       (link-to "/login" "Log in"))]
  (html5
    [:head [:title "Movie Advisor!!!"]
    ;(include-css "/css/screen.css")
    (include-css "/css/top.css")
    (include-css "http://yui.yahooapis.com/pure/0.5.0/pure-min.css")]
    [:body 
     [:div#container 
      [:div#header [:h1 {:align "center"} "Movie Advisor"]]
      [:div#navigation [:ul
                        [:li login-or-out] [:li (link-to "/" "Home")]
       ]
      ]
      [:div#content body][:div#footer "Copyright WibiData 2014"]]])))

