(ns movie-advisor.layout
  (:require [selmer.parser :as parser]
            [clojure.string :as s]
            [hiccup.page  :refer [html5 include-css]]
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
  (html5
    [:head [:title "Movie Advisor!!!!!!!!"]
    (include-css "/css/screen.css")]
    [:body body]))

