(ns opentsdb-demo.uid
  (:require [clojure-hbase.core :as hb]
            [clojure.tools.logging :as logging])
  (:import (org.apache.hadoop.hbase.util Bytes)
           (org.apache.hadoop.hbase.client Scan ResultScanner Result)))

(defonce UID-TABLE-NAME "tsdb-uid")
(defonce MAX-UID-ROW-KEY (hb/to-bytes 0x00))
(defonce UID-SIZE-BYTES 3)

(defn- map-fn [bytes]
  (keyword (Bytes/toString bytes)))

(defn- verify-uid [uid]
  (if (<= uid 0xFFFFFF)
    uid
    (throw (IllegalStateException. "uid overflow"))))

(defn- to-standard-uid-bytes [uid]
  (let [uid (hb/to-bytes uid)]
    (->> (drop (- (count uid) UID-SIZE-BYTES) uid)
         (byte-array UID-SIZE-BYTES))))

(defn get-or-createId [name kind]
  (try
    (hb/with-table [tsdb-uid (hb/table UID-TABLE-NAME)]
      (if-let [uid (not-empty (hb/latest-as-map (hb/get tsdb-uid name :columns [:id [kind]])
                                :map-family map-fn :map-qualifier map-fn))]
        (get-in uid [:id kind])
        (when-let [new-uid (.incrementColumnValue tsdb-uid MAX-UID-ROW-KEY
                             (hb/to-bytes :id) (hb/to-bytes kind) (long 1))]
          (let [new-uid (-> new-uid verify-uid to-standard-uid-bytes)]
            (hb/put tsdb-uid new-uid :values [:name [kind (hb/to-bytes name)]])
            (hb/put tsdb-uid name :values [:id [kind (hb/to-bytes new-uid)]])
            new-uid))))
    (catch Exception e
      (logging/errorf e "Get or create uid for name %s kind %s failed" name kind))))

(defn ^ResultScanner getSuggestScanner [search kind]
  (let [[start-row end-row] (if (seq search)
                              (let [start-row (hb/to-bytes search)
                                    end-row   (byte-array
                                                (assoc (vec start-row)
                                                       (- (count start-row) 1)
                                                       (inc (last start-row))))]
                                [start-row end-row])
                              [(byte 0) (byte 255)])
        ^Scan scan (doto (Scan.)
                     (.setStartRow start-row)
                     (.setStopRow end-row)
                     (.addColumn (hb/to-bytes :id) (hb/to-bytes kind))
                     (.setMaxResultSize 10))]
    (hb/with-table [tsdb-uid (hb/table UID-TABLE-NAME)]
      (hb/scanner tsdb-uid scan))))

(defn suggest [search kind]
  {:pre [(contains? #{:metrics :tag} kind)]}
  (let [scanner (getSuggestScanner search kind)]
    (try
      (loop [ret (.next scanner) list []]
        (if ret
          (when-let [row (.getRow ^Result ret)]
            (recur (.next scanner) (conj list (String. row))))
          list))
      (catch Exception e
        (logging/error e "Get result from scanner failed."))
      (finally
        (.close scanner)))))
