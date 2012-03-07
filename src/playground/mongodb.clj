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
  [input-uri output-uri fields]
    (cascading.mongodb.MongoTap. input-uri output-uri (make-fields fields)))

(def person (create-mongo-tap "mongodb://127.0.0.1/test.person" "mongodb://127.0.0.1/test.out" ["_id" "name"]))

(def age (create-mongo-tap "mongodb://127.0.0.1/test.age" "mongodb://127.0.0.1/test.out" ["name" "age"]))

(def gender (create-mongo-tap "mongodb://127.0.0.1/test.gender" "mongodb://127.0.0.1/test.out" ["name" "gender"]))

(def follows (create-mongo-tap "mongodb://127.0.0.1/test.follows" "mongodb://127.0.0.1/test.out" ["person_follower" "person_followed"]))

(def location (create-mongo-tap "mongodb://127.0.0.1/test.location" "mongodb://127.0.0.1/test.out" ["person" "country" "state" "city"]))




;(with-debug (?<- (stdout) [?id ?name] (person ?id ?name)))
;(?<- (stdout) [?id ?name] (person ?id ?name))