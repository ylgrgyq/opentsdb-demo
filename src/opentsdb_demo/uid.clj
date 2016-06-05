(ns opentsdb-demo.uid
  (:require [clojure-hbase.core :as hb])
  (:import (org.apache.hadoop.hbase.util Bytes)))

(defonce ^:private UID-TABLE-NAME "tsdb-uid")
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
