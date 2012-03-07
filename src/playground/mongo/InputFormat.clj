(ns playground.mongo.InputFormat
  (:gen-class
    :name playground.mongo.InputFormat
    :implements [org.apache.hadoop.mapred.InputFormat])
  (:use somnium.congomongo))

(defn get-connection [conf]
  (make-connection (.get conf "mongodbTap.database")
    (.get conf "mongodbTap.host")
    (.get conf "mongodbTap.port")))

(defn make-record-reader [records]
  (println "make-record-reader: " records)
  (let [num (count records)
        position (atom 0)
        cursor (atom records)]
    (proxy [org.apache.hadoop.mapred.RecordReader] []
      (next [k v]
        (if (< 0 (count @cursor))
          (let [next-record (first records)]
            (set! (. k object) (next-record "_id"))
            (.clear v)
            (.putAll v next-record)
            (.remove v "_id")
;            (println "make-record-reader built: " k " -> " v)
            (dosync
              (swap! position + 1)
              (swap! cursor rest))
            true)
          false))
      (createKey [] (playground.ObjectHolder.))
      (createValue [] (java.util.HashMap.))
      (getPos [] @position)
      (close [] nil)
      (getProgress [] (if (= 0 num) 1 (/ (* @position 1.0) num))))))


(defn get-query [conf]
  (cascalog.KryoService/deserialize (.getBytes (.get conf "mongodbTap.query"))))

(defn -getSplits [this conf i]
  (let [query (get-query conf)
        num (with-mongo (get-connection conf) (apply fetch-count query))]
    (println "counted " num " records for query: " query)
    (into-array [(playground.mongo.InputSplit. num)])))

(defn -getRecordReader [this is conf reporter]
  (make-record-reader
    (with-mongo (get-connection conf) (apply fetch (get-query conf)))))

