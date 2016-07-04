(defproject opentsdb-demo "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.0"]
                 [clojure-hbase "1.0.0-cdh5.4.7-SNAPSHOT" :exclusions [org.jruby/jruby-complete org.slf4j/slf4j-log4j12]]
                 [org.slf4j/slf4j-log4j12 "1.7.2"]
                 [org.slf4j/slf4j-api "1.7.2"]
                 [clojurewerkz/buffy "1.0.2"]
                 [environ "0.5.0"]]
  :repositories [["bitwalker.user-agent-utils.mvn.repo" "https://raw.github.com/HaraldWalker/user-agent-utils/mvn-repo/"]
                 ["maven.mei.fm" "http://maven.mei.fm/nexus/content/groups/public/"]]
  :main ^:skip-aot opentsdb-demo.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
