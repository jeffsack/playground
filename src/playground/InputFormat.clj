(ns playground.InputFormat
  (:gen-class
    :name playground.InputFormat
    :implements [org.apache.hadoop.mapred.InputFormat]))

(defn make-record-reader []
  (let [count (atom 5)]
    (proxy [org.apache.hadoop.mapred.RecordReader] []
      (next [k v] (<= 0 (swap! count - 1)))
      (createKey [] {})
      (createValue [] (org.apache.hadoop.io.NullWritable/get))
      (getPos [] 0)
      (close [] nil)
      (getProgress [] 1.0))))


(defn -getSplits [this jc i]
  (into-array [(playground.InputSplit.)]))

(defn -getRecordReader [this is jc reporter]
  (make-record-reader))



;(compile 'playground.InputSplit)
;
;(compile 'playground.InputFormat)
;
;(defn uuid [] (.toString (java.util.UUID/randomUUID)))
;
;(defn make-fields [fields] (cascading.tuple.Fields. (into-array Comparable fields)))
;
;(defn make-memory-scheme [data]
;  (let [fs (make-fields (keys data))]
;    (proxy [cascading.scheme.Scheme] [fs]
;      (sourceInit [tap conf]
;        (org.apache.hadoop.mapred.FileInputFormat/setInputPaths conf (str "/" (uuid)))
;        (.setInputFormat conf playground.InputFormat))
;      (source [key value]
;        (println "received key: " key "; and value: " value)
;        (cascading.tuple.Tuple. (into-array Object (vals data)))))))
;
;(defn make-memory-tap [data]
;  (let [id (uuid)
;        scheme (make-memory-scheme data)]
;    (proxy [cascading.tap.SourceTap] [scheme]
;      (getPath [] (str "/" id))
;      (pathExists [jc] true)
;      (getPathModified [jc] (System/currentTimeMillis)))))