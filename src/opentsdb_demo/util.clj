(ns opentsdb-demo.util
  (:import (org.apache.hadoop.hbase.util Bytes)))

(defn bytes-to-string [v]
  (Bytes/toString v))

(defn bytes-to-long [v]
  (Bytes/toLong v))

(defn bytes-to-int [v]
  (Bytes/toInt v))

(defn bytes-to-number [v]
  (if (= (count v) 8)
    (bytes-to-long v)
    (bytes-to-int v)))
