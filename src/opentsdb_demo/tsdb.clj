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

(def uid-frame
  (bf/frame-type
    (bf/frame-encoder [value]
      bytes (bc/bytes-type uid/UID-SIZE-BYTES) value)
    (bf/frame-decoder [buffer offset]
      bytes (bc/bytes-type uid/UID-SIZE-BYTES))
    first))

(def tags-frame-type
  (let [tag-val-pair (bf/composite-frame uid-frame uid-frame)]
    (bf/frame-type
      (bf/frame-encoder [value]
        length (bc/int32-type) (count value)
        tags (bf/repeated-frame tag-val-pair (count value)) value)
      (bf/frame-decoder [buffer offset]
        length (bc/int32-type)
        tags (bf/repeated-frame tag-val-pair (bp/read length buffer offset)))
      second)))

(def rowkey-frame-type
  (bf/frame-type
    (bf/frame-encoder [value]
      metric (bc/bytes-type 3) (first value)
      base-time (bc/int32-type) (second value)
      tags tags-frame-type (nth value 2))
    (bf/frame-decoder [buffer offset]
      metric (bc/bytes-type 3)
      base-time (bc/int32-type)
      tags tags-frame-type)))

(defn set-or-create-tags [buf tags]
  (let [buf (byte-array (* 6 (count tags)))]
    (-> (map (fn [tag index]
               (let [[tag-name tag-val] tag
                     tag-name-uid (uid/get-or-createId tag-name :tag-name)
                     tag-val-uid  (uid/get-or-createId tag-val :tag-val)]
                 (Arrays/fill buf index (+ index 3) tag-name-uid)
                 (Arrays/fill buf index (+ index 3) tag-val-uid)
                 [tag-name-uid tag-val-uid])) (sort tags) (range (count tags)))
      clojure.string/join)))

(defn row-key [metric base-time tags]
  (let [buf (bc/compose-buffer (bc/spec :metric (bc/bytes-type 3)
                                        :base-time (bc/int32-type)
                                              :tags-filed (bc/repeated-type
                                                            (bc/bytes-type 6)
                                                            (count tags))))
        metric-uid (uid/get-or-createId metric :metric)
        tags-uids (set-or-create-tags buf tags)]
    (bc/set-field buf :metric metric-uid)
    (bc/set-field buf :base-time base-time)
    (.array buf)))

(defn add-point [metric timestamp value tags]
  (let [base-time (- timestamp (quot timestamp MAX_TIME_SPAN_SEC))
        row (row-key metric base-time tags)
        flag (if (float? value) 0xB 0x7)
        quilifier (bit-or (bit-shift-left (- timestamp base-time) 4) flag)]
    (hb/with-table [tsdb-table (hb/table TSDB_TABLE_NAME)]
      (hb/put tsdb-table row :values [:t [quilifier value]]))))


