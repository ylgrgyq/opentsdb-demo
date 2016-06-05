(ns opentsdb-demo.tsdb
  (:require [clojure-hbase.core :as hb]
            [clojurewerkz.buffy.core :as bc]
            [clojurewerkz.buffy.frames :as bf]
            [clojurewerkz.buffy.types.protocols :as bp]
            [opentsdb-demo.uid :as uid]
            [opentsdb-demo.buffer :as buffer]
            [opentsdb-demo.util :as u])
  (:import (org.apache.hadoop.hbase.util Bytes)
           (java.util Arrays)))

(def ^:private TIMESTAMP_BYTES 4)
(def ^:private MAX_TIME_SPAN_SEC 3600)
(def ^:private TSDB_TABLE_NAME "tsdb")
(def ^:ptivate FLOAT_VAL_FLAG 0xB)
(def ^:ptivate LONG_VAL_FLAG 0x7)

(def ^:private uid-frame
  (bf/frame-type
    (bf/frame-encoder [value]
      bytes (bc/bytes-type uid/UID-SIZE-BYTES) value)
    (bf/frame-decoder [buffer offset]
      bytes (bc/bytes-type uid/UID-SIZE-BYTES))
    first))

(def ^:private tags-frame-type
  (let [tag-val-pair (bf/composite-frame uid-frame uid-frame)]
    (bf/frame-type
      (bf/frame-encoder [value]
        length (bc/int32-type) (count value)
        tags (bf/repeated-frame tag-val-pair (count value)) value)
      (bf/frame-decoder [buffer offset]
        length (bc/int32-type)
        tags (bf/repeated-frame tag-val-pair (bp/read length buffer offset)))
      second)))

(def ^:private rowkey-frame-type
  (bf/frame-type
    (bf/frame-encoder [value]
      metric (bc/bytes-type uid/UID-SIZE-BYTES) (first value)
      base-time (bc/int32-type) (second value)
      tags tags-frame-type (nth value 2))
    (bf/frame-decoder [buffer offset]
      metric (bc/bytes-type uid/UID-SIZE-BYTES)
      base-time (bc/int32-type)
      tags tags-frame-type)))

(defn create-tags [tags]
  (map #(let [[tag-name tag-val] %
              tag-name-uid (uid/get-or-createId tag-name :tag-name)
              tag-val-uid  (uid/get-or-createId tag-val :tag-val)]
         [tag-name-uid tag-val-uid]) (sort tags)))

(defn row-key [metric base-time tags]
  (let [buf (buffer/dynamic-heap-buffer rowkey-frame-type)
        metric-uid (uid/get-or-createId metric :metrics)
        tags-uids (create-tags tags)
        buf (bc/compose buf [[metric-uid base-time tags-uids]])]
    (buffer/byte-buf->bytes buf)))

(defn add-point [metric timestamp value tags]
  (let [base-time (- timestamp (quot timestamp MAX_TIME_SPAN_SEC))
        row (row-key metric base-time tags)
        flag (if (float? value) FLOAT_VAL_FLAG LONG_VAL_FLAG)
        quilifier (bit-or (bit-shift-left (- timestamp base-time) 4) flag)]
    (hb/with-table [tsdb-table (hb/table TSDB_TABLE_NAME)]
      (hb/put tsdb-table row :values [:t [quilifier value]]))))


