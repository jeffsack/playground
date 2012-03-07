(ns playground.mongo.InputSplit
  (:gen-class
    :name playground.mongo.InputSplit
    :implements [org.apache.hadoop.mapred.InputSplit]
    :init init
    :constructors {[] [] [Long] []}
    :state state))

(defn -init
  ([] [[] nil])
  ([num] [[] (ref num)]))
(defn -getLength [this] @(.state this))
(defn -getLocations [this] (into-array String []))
(defn -write [this d] (.writeInt d @(.state this)))
(defn -readFields [this ^java.io.DataInput di] (.readInt di))

