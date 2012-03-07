(ns playground.mongodb
  (:use cascalog.api
        somnium.congomongo
        playground.test-data)
  (:require [cascalog.io :as io]
            [cascalog.api :as api]
            [cascalog.workflow :as w]
            [cascalog.vars :as v]
            [cascalog.ops :as c]))

(def conn (make-connection "test" :host "127.0.0.1" :port 27017))

(defn delete-collections [conn colls]
  (map #(with-mongo conn (drop-coll! %)) colls))


(defn make-maps [columns values]
  (map (fn [row] (zipmap columns row)) values))

(defn insert-data! [conn coll columns values]
  (let [records (map (fn [row] (zipmap columns row)) values)]
    (map #(with-mongo conn (insert! coll %)) records)))

(defn make-fields [fields] (cascading.tuple.Fields. (into-array Comparable fields)))

(defn create-mongo-tap
  [host port db coll fields]
    (playground.mongodb.MongoDBSourceTap. host port db coll (make-fields fields)))

(def person (create-mongo-tap "127.0.0.1" 27017 "test" "person" ["_id" "name"]))

(def age (create-mongo-tap "127.0.0.1" 27017 "test" "age" ["name" "age"]))

(def gender (create-mongo-tap "127.0.0.1" 27017 "test" "gender" ["name" "gender"]))

(def follows (create-mongo-tap "127.0.0.1" 27017 "test" "follows" ["person_follower" "person_followed"]))

(def location (create-mongo-tap "127.0.0.1" 27017 "test" "location" ["person" "country" "state" "city"]))





(compile 'playground.mongo.InputSplit)

(compile 'playground.mongo.InputFormat)

(defn uuid [] (.toString (java.util.UUID/randomUUID)))

(defn make-fields [fields] (cascading.tuple.Fields. (into-array Comparable fields)))

(defn make-mongo-scheme [host port database query fields]
  (let [fs (make-fields fields)]
    (proxy [cascading.scheme.Scheme] [fs]
      (sourceInit [tap conf]
        (println "sourceInit(" tap ", " conf ")")
        (org.apache.hadoop.mapred.FileInputFormat/setInputPaths conf (str "/" (uuid)))
        (println "here 1")
        (.setInputFormat conf playground.mongo.InputFormat)
        (println "here 2")
        (doto conf
          (.set "mongodbTap.host" host)
          (.setInt "mongodbTap.port" port)
          (.set "mongodbTap.database" database)
          (.set "mongodbTap.query" (String. (cascalog.KryoService/serialize query))))
        (println "here 3"))
      (source [key value]
        (println "source(" key ", " value ")")
        (let [values (map #(if (= % "_id")
                             (str (.object key))
                             (value %)) fields)]
          (println "received key: " key "; and value: " value)
          (cascading.tuple.Tuple. (into-array Object values)))))))

(defn make-mongo-tap [host port database query fields]
  (let [id (uuid)
        scheme (make-mongo-scheme host port database query fields)]
    (proxy [cascading.tap.SourceTap] [scheme]
      (getPath [] (str "/" id))
      (pathExists [conf] true)
      (getPathModified [conf] (System/currentTimeMillis)))))


(def person (make-mongo-tap "127.0.0.1" 27017 "test" ["person"] ["_id" "name"]))


(with-debug (?<- (stdout) [?id ?name] (person ?id ?name)))


(<- [?id ?name] (person ?id ?name))