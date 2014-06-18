(defproject movie-advisor "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :repositories {
                 "kiji repo" "https://repo.wibidata.com/artifactory/kiji",
                 "kiji nightly" "https://repo.wibidata.com/artifactory/kiji-nightly",
                 }
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [lib-noir "0.8.3"]
                 [ring-server "0.3.1"]
                 [selmer "0.6.6"]
                 [com.taoensso/timbre "3.2.1"]
                 [com.taoensso/tower "2.0.2"]
                 [markdown-clj "0.9.44"]
                 [environ "0.5.0"]
                 ; Talk to Kiji through KijiSchema
                 [org.kiji.schema/kiji-schema "1.4.3-SNAPSHOT"]
                 [org.kiji.scoring/kiji-scoring "0.15.0-SNAPSHOT"]
                 ; Provided dependency for Kiji
                 [org.apache.hadoop/hadoop-client "2.0.0-mr1-cdh4.3.0"]
                 [org.apache.hbase/hbase "0.94.6-cdh4.3.0"]
                 ; Avro records!
                 [org.kiji.tutorial/movie-advisor-avro "1.0-SNAPSHOT"]
                 ; The score function
                 [org.kiji.tutorial/movie-advisor-scoring "1.0-SNAPSHOT"]
                 [log4j/log4j "1.2.16" :exclusions [javax.mail/mail javax.jms/jms com.sun.jdmk/jmxtools com.sun.jmx/jmxri]]
                 ]
  ; Turn on to figure out what is happening with log4j
  ;:jvm-opts ["-Dlog4j.debug"]

  :repl-options {:init-ns movie-advisor.repl}
  :plugins [[lein-ring "0.8.10"]
            [lein-environ "0.5.0"]]
  :ring {:handler movie-advisor.handler/app
         :init    movie-advisor.handler/init
         :destroy movie-advisor.handler/destroy}
  :profiles
  {:uberjar {:aot :all}
   :production {:ring {:open-browser? false
                       :stacktraces?  false
                       :auto-reload?  false}}
   :dev {:dependencies [[ring-mock "0.1.5"]
                        [ring/ring-devel "1.2.2"]]
         :env {:dev true}}}
  :min-lein-version "2.0.0")
