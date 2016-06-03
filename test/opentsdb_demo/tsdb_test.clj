(ns opentsdb-demo.tsdb-test
  (:require [clojure.test :refer :all]
            [opentsdb-demo.tsdb :refer :all]
            [clojurewerkz.buffy.core :as bc]
            [opentsdb-demo.buffer :as buffer]))

(deftest test-frames
  (let [metric "met"
        base-time 1234567
        tags [["tg1" "vl1"] ["tg2" "vl2"]]
        rowkey [[(.getBytes metric)
                 base-time
                 (mapv (fn [v] (mapv #(.getBytes %) v)) tags)]]
        dynamic-type (buffer/dynamic-heap-buffer rowkey-frame-type)
        buf (bc/compose dynamic-type rowkey)]
    (let [[[ret-metric ret-time ret-tags]] (bc/decompose dynamic-type buf)]
      (are [x y] (= x y)
                 metric (String. ret-metric)
                 base-time ret-time
                 tags (mapv (fn [v] (mapv #(String. %) v)) ret-tags)))))
