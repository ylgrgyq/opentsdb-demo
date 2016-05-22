(ns opentsdb-demo.uid-test
  (:require [clojure.test :refer :all]
            [opentsdb-demo.uid :refer :all]))

(deftest test-get-or-creat-id
  (get-or-createId "mysql.guorui.haha" :metrics))
