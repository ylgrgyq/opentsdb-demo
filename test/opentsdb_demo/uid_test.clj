(ns opentsdb-demo.uid-test
  (:require [clojure.test :refer :all]
            [opentsdb-demo.uid :refer :all]
            [clojure-hbase.core :as hb]))

(deftest test-get-or-creat-id
  (println (get-or-createId "mysd" :metrics))
  (println (get-or-createId "mtsdq" :metrics)))
