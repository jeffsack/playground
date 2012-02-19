(ns playground.core
  (:use cascalog.api
;        cascalog.tap
        clojure.test
        cascalog.testing)
  (:require [cascalog.io :as io]
            [cascalog.api :as api]
            [cascalog.workflow :as w]
            [cascalog.vars :as v]
            [cascalog.ops :as c]
            [clojure.java.jdbc :as sql])
  (:import [cascading.tuple Fields]
           [cascading.tap Hfs Lfs GlobHfs TemplateTap Tap SinkMode]
           [cascading.jdbc TableDesc JDBCTap JDBCScheme]
           [org.h2 Driver]
           [java.sql DriverManager]))


(def url "jdbc:h2:.tempdata")
(def driver-name "org.h2.Driver")
(def username "sa")
(def password "")

(Class/forName driver-name)

(def datasource (org.springframework.jdbc.datasource.LazyConnectionDataSourceProxy.
                  (let [simpleDatasource (org.springframework.jdbc.datasource.SimpleDriverDataSource.)]
                    (.setDriverClass simpleDatasource (Class/forName driver-name))
                    (.setUrl simpleDatasource url)
                    (.setUsername simpleDatasource username)
                    (.setPassword simpleDatasource password)
                    simpleDatasource)))

(def db {:datasource datasource})

(defn setup []
  (sql/with-connection db
    (sql/do-commands
      "create or replace table PERSON (ID INT PRIMARY KEY AUTO_INCREMENT, NAME VARCHAR(255))"
      "create or replace table AGE (NAME VARCHAR(255), AGE INT)"
      "create or replace table GENDER (NAME VARCHAR(255), GENDER CHAR(1))"
      "create or replace table FOLLOWS (person_follower VARCHAR(255), person_followed VARCHAR(255))"
      "create or replace table LOCATION (person VARCHAR(255), country VARCHAR(255), state VARCHAR(255), city VARCHAR(255))")))

(defn tear-down []
  (sql/with-connection db
    (sql/do-commands
      "drop table if exists PERSON"
      "drop table if exists AGE"
      "drop table if exists GENDER"
      "drop table if exists FOLLOWS"
      "drop table if exists LOCATION")))


(defn create-jdbc-tap [table-name column-names column-defs primary-keys]
  (let [table-desc
        (TableDesc. table-name (into-array String column-names)
          (into-array String column-defs) (into-array String primary-keys))]
    (JDBCTap. url username password driver-name table-name (JDBCScheme. (into-array String column-names)))))

(defn insert-data [db table-name columns values]
  (let [records (map (fn [row] (zipmap columns row)) values)]
    (map #(sql/with-connection db (sql/insert-record table-name %)) records)))

(def person (create-jdbc-tap "person" ["id" "name"] ["int not null" "varchar(100) not null"] ["id"]))

(def age (create-jdbc-tap "age" ["name" "age"] ["varchar(100) not null" "int not null"] ["name"]))

(def gender (create-jdbc-tap "gender" ["name" "gender"] ["varchar(100) not null" "char(1) not null"] ["name"]))

(def follows (create-jdbc-tap "follows" ["person_follower" "person_followed"] ["varchar(100) not null" "varchar(100) not null"] ["person_follower" "person_followed"]))

(def location (create-jdbc-tap "location" ["person" "country" "state" "city"] ["varchar(100) not null" "varchar(100) not null" "varchar(100) not null" "varchar(100) not null"] ["person"]))

(def person-data
  [
    ;; [person]
    ["alice"]
    ["bob"]
    ["chris"]
    ["david"]
    ["emily"]
    ["george"]
    ["gary"]
    ["harold"]
    ["kumar"]
    ["luanne"]
    ])

(def age-data
  [
    ;; [person age]
    ["alice" 28]
    ["bob" 33]
    ["chris" 40]
    ["david" 25]
    ["emily" 25]
    ["george" 31]
    ["gary" 28]
    ["kumar" 27]
    ["luanne" 36]
    ])

(def gender-data
  [
    ;; [person gender]
    ["alice" "f"]
    ["bob" "m"]
    ["chris" "m"]
    ["david" "m"]
    ["emily" "f"]
    ["george" "m"]
    ["gary" "m"]
    ["harold" "m"]
    ["luanne" "f"]
    ])

(def follows-data
  [
    ;; [person-follower person-followed]
    ["alice" "david"]
    ["alice" "bob"]
    ["alice" "emily"]
    ["bob" "david"]
    ["bob" "george"]
    ["bob" "luanne"]
    ["david" "alice"]
    ["david" "luanne"]
    ["emily" "alice"]
    ["emily" "bob"]
    ["emily" "george"]
    ["emily" "gary"]
    ["george" "gary"]
    ["harold" "bob"]
    ["luanne" "harold"]
    ["luanne" "gary"]
    ])

(def location-data
  [
    ;; [person country state city]
    ["alice" "usa" "california" nil]
    ["bob" "canada" nil nil]
    ["chris" "usa" "pennsylvania" "philadelphia"]
    ["david" "usa" "california" "san francisco"]
    ["emily" "france" nil nil]
    ["gary" "france" nil "paris"]
    ["luanne" "italy" nil nil]
    ])

(setup)

(insert-data db "person" ["name"] person-data)

(insert-data db "age" ["name" "age"] age-data)

(insert-data db "gender" ["name" "gender"] gender-data)

(insert-data db "follows" ["person_follower" "person_followed"] follows-data)

(insert-data db "location" ["person" "country" "state" "city"] location-data)

;(sql/with-connection db
;  (sql/with-query-results rs ["select * from PERSON"]
;    (dorun (map #(println %) rs))))


(?<- (stdout) [?person] (age ?person 25))

(?<- (stdout) [?person] (age ?person ?age) (< ?age 30))

(?<- (stdout) [?person ?age] (age ?person ?age)
         (< ?age 30))

(?<- (stdout) [?person] (follows "emily" ?person)
         (gender ?person "m"))

(?<- (stdout) [?person ?a2] (age ?person ?age)
  (< ?age 30) (* 2 ?age :> ?a2))

(?<- (stdout) [?person1 ?person2]
  (age ?person1 ?age1) (follows ?person1 ?person2)
  (age ?person2 ?age2) (< ?age2 ?age1))

(?<- (stdout) [?person1 ?person2 ?delta]
  (age ?person1 ?age1) (follows ?person1 ?person2)
  (age ?person2 ?age2) (- ?age2 ?age1 :> ?delta)
  (< ?delta 0))

(?<- (stdout) [?count] (age _ ?a) (< ?a 30)
  (c/count ?count))

(?<- (stdout) [?person ?count] (follows ?person _)
  (c/count ?count))

(?<- (stdout) [?country ?avg]
  (location ?person ?country _ _) (age ?person ?age)
  (c/count ?count) (c/sum ?age :> ?sum)
  (div ?sum ?count :> ?avg))

(tear-down)

; TODO: clean this up and reorganize a bit better, move appropriate stuff into test dir