(ns opentsdb-demo.uid
  (:require [clojure-hbase.core :as hb]
            [opentsdb-demo.util :as u])
  (:import (org.apache.hadoop.hbase.util Bytes)))

(defonce ^:private UID-TABLE-NAME "tsdb-uid")
(defonce ^:private MAX-UID-ROW-KEY 0x00)

(defn- map-fn [bytes]
  (keyword (Bytes/toString bytes)))

(defn get-or-createId [name kind]
  (hb/with-table [tsdb-uid (hb/table UID-TABLE-NAME)]
    (if-let [uid (not-empty (hb/latest-as-map (hb/get tsdb-uid name :columns [:id [kind]])
                              :map-family map-fn :map-qualifier map-fn))]
      (u/bytes-to-number (get-in uid [:id kind]))
      (let [new-uid (.incrementColumnValue tsdb-uid (hb/to-bytes MAX-UID-ROW-KEY)
                      (hb/to-bytes :id) (hb/to-bytes kind) (long 1))]
        (hb/put tsdb-uid new-uid :values [:name [kind name]])
        (hb/put tsdb-uid name :values [:id [kind new-uid]])
        new-uid))))
