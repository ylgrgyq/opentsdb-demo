(ns opentsdb-demo.buffer
  (:require [clojurewerkz.buffy.types.protocols :as bp]
            [clojurewerkz.buffy.core :as bc]
            [clojurewerkz.buffy.frames :as bf]))

(deftype DynamicHeapBuffer [frames]
  bc/Composable
  (compose [this values]
    (let [size   (bf/encoding-size frames values)
          buffer (bc/heap-buffer size)]
      (bp/write frames buffer 0 values)))

  (decompose [this buffer]
    (bp/read frames buffer 0)))

(defn dynamic-heap-buffer
  [& frames]
  (DynamicHeapBuffer. (apply bf/composite-frame frames)))
