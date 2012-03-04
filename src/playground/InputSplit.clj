(ns playground.InputSplit
  (:gen-class
    :name playground.InputSplit
    :implements [org.apache.hadoop.mapred.InputSplit]))

(defn -getLength [this] 1)
(defn -getLocations [this] (into-array String []))
(defn -write [this d] (.writeInt d 1))
(defn -readFields [this ^java.io.DataInput di] (.readInt di))

