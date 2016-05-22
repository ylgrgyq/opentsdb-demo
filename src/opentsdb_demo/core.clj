(ns opentsdb-demo.core
  (:gen-class)
  (:require [clojure-hbase.core :as hb]
            [environ.core :refer [env]]))

(defonce config
  (hb/make-config
    {:hbase.zookeeper.quorum (env :hbase-zookeeper-url "127.0.0.1")
     :hbase.client.scanner.caching "10"}))

(defn init-hbase-config []
  (hb/set-config config))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))
