(ns playground.jdbc
  (:use cascalog.api
        clojure.test
        cascalog.testing
        playground.test-data)
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
           [java.sql DriverManager])
  (:refer-clojure :exclude [count doc]))

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

(defn insert-data! [db table-name columns values]
  (let [records (map (fn [row] (zipmap columns row)) values)]
    (map #(sql/with-connection db (sql/insert-record table-name %)) records)))

(defn setup []
  (sql/with-connection db
     (sql/do-commands
       "create or replace table PERSON (ID INT PRIMARY KEY AUTO_INCREMENT, NAME VARCHAR(255))"
       "create or replace table AGE (NAME VARCHAR(255), AGE INT)"
       "create or replace table GENDER (NAME VARCHAR(255), GENDER CHAR(1))"
       "create or replace table FOLLOWS (person_follower VARCHAR(255), person_followed VARCHAR(255))"
       "create or replace table LOCATION (person VARCHAR(255), country VARCHAR(255), state VARCHAR(255), city VARCHAR(255))")))


(sql/with-connection db
  (sql/with-query-results rs ["select * from PERSON"]
    (dorun (map #(println %) rs))))

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


(def person (create-jdbc-tap "person" ["id" "name"] ["int not null" "varchar(100) not null"] ["id"]))

(def age (create-jdbc-tap "age" ["name" "age"] ["varchar(100) not null" "int not null"] ["name"]))

(def gender (create-jdbc-tap "gender" ["name" "gender"] ["varchar(100) not null" "char(1) not null"] ["name"]))

(def follows (create-jdbc-tap "follows" ["person_follower" "person_followed"] ["varchar(100) not null" "varchar(100) not null"] ["person_follower" "person_followed"]))

(def location (create-jdbc-tap "location" ["person" "country" "state" "city"] ["varchar(100) not null" "varchar(100) not null" "varchar(100) not null" "varchar(100) not null"] ["person"]))


(tear-down)

(setup)

(insert-data! db "person" ["name"] person-data)
(insert-data! db "age" ["name" "age"] age-data)
(insert-data! db "gender" ["name" "gender"] gender-data)
(insert-data! db "follows" ["person_follower" "person_followed"] follows-data)
(insert-data! db "location" ["person" "country" "state" "city"] location-data)



