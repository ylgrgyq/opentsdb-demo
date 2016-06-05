(ns opentsdb-demo.tsdb-test
  (:require [clojure.test :refer :all]
            [opentsdb-demo.tsdb :refer :all]
            [clojurewerkz.buffy.core :as bc]
            [opentsdb-demo.buffer :as buffer]
            [clojure-hbase.core :as hb]
            [opentsdb-demo.uid :as uid])
  (:import (java.util.concurrent TimeUnit)
           (io.netty.buffer ByteBuf)))

(deftest test-row-key
  (let [metric "met"
        time (.toSeconds TimeUnit/MILLISECONDS (System/currentTimeMillis))
        base-time (- time (quot time @#'opentsdb-demo.tsdb/MAX_TIME_SPAN_SEC))
        tags [["tg1" "vl1"] ["tg2" "vl2"]]]
    (with-redefs [create-tags (fn [t]
                                (is (= tags t))
                                (mapv (fn [v] (mapv #(.getBytes %) v)) tags))
                  uid/get-or-createId (fn [name kind]
                                        (are [x y] (= x y)
                                                   metric name
                                                   :metric kind)
                                        (.getBytes metric))]
      (let [buffer (buffer/dynamic-heap-buffer @#'opentsdb-demo.tsdb/rowkey-frame-type)
            rowkey       (row-key metric base-time tags)
            _ (println rowkey (count rowkey))
            ^ByteBuf buf (bc/heap-buffer (count rowkey))
            buf          (.setBytes buf 0 rowkey 0 (count rowkey))
            [[ret-metric ret-time ret-tags]] (bc/decompose buffer buf)]
        (are [x y] (= x y)
                   metric (String. ret-metric)
                   base-time ret-time
                   tags (mapv (fn [v] (mapv #(String. %) v)) ret-tags))))))
