(ns movie-advisor.test.handler
  (:use clojure.test
        ring.mock.request
        movie-advisor.handler)
  (:require [movie-advisor.kiji :as kiji]
            [clojure.java.io]
            [taoensso.timbre :as timbre])
  (:import (org.kiji.schema KijiClientTest Kiji KijiURI KijiTable)
           (org.kiji.schema.util InstanceBuilder)
           (org.kiji.schema.shell.api Client)))

(defn setup-test-kiji
  "Use KijiClientTest to set up a test Kiji instance."
  [f]

  (timbre/info "Doing one-time test setup.")
  
  ; Create a test Kiji instance.
  (def kiji-client-test (KijiClientTest.))
  (.setupKijiTest kiji-client-test)
  (def kiji-instance (.getKiji kiji-client-test))

  (timbre/info "Created Kiji instance " kiji-instance ".")

  ; Create the users and movies tables.
  (let [client (Client/newInstance (.getURI kiji-instance))]
    (.executeUpdate client (slurp (clojure.java.io/resource "users.ddl")))
    (.executeUpdate client (slurp (clojure.java.io/resource "movies.ddl"))))

  (timbre/info "Created users and movies tables.")

  (with-redefs [kiji/open-kiji-instance (fn [] kiji-instance)]
    (f))

  (timbre/info "Tearing down Kiji instance.")
  ; Close the references in kiji namespace to the users and movies tables.
  (.teardownKijiTest kiji-client-test)
  (timbre/info "All done with tests!")
)


(use-fixtures :once setup-test-kiji)

(deftest test-app
  (testing "main route"
    (let [response (app (request :get "/"))]
      (is (= (:status response) 200))))

  (testing "not-found route"
    (let [response (app (request :get "/invalid"))]
      (is (= (:status response) 404)))))

(deftest test-kiji-stuff
  (testing "get URI from kiji"
    (timbre/info "Inside test to get Kiji URI")
    (kiji/init)
    (let [kiji-uri (.getURI kiji/kiji)
          kiji-uri-string (.toString kiji-uri)]
      (timbre/info "Got URI " kiji-uri-string)
      (is (.startsWith kiji-uri-string "kiji://"))))
  )
