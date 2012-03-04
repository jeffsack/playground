(ns playground.core
  (:use cascalog.api
        playground.test-data)
  (:require [cascalog.io :as io]
            [cascalog.api :as api]
            [cascalog.workflow :as w]
            [cascalog.vars :as v]
            [cascalog.ops :as c]
            [playground.jdbc :as jdbc]
            [playground.mongodb :as mongo])
  (:refer-clojure :exclude [count doc]))

(jdbc/tear-down)
(jdbc/setup)
(jdbc/insert-data! jdbc/db "person" ["name"] person-data)
(jdbc/insert-data! jdbc/db "age" ["name" "age"] age-data)
(jdbc/insert-data! jdbc/db "gender" ["name" "gender"] gender-data)
(jdbc/insert-data! jdbc/db "follows" ["person_follower" "person_followed"] follows-data)
(jdbc/insert-data! jdbc/db "location" ["person" "country" "state" "city"] location-data)

(mongo/delete-collections mongo/conn ["person" "age" "gender" "follows" "location"])
(mongo/insert-data! mongo/conn "person" ["name"] person-data)
(mongo/insert-data! mongo/conn "age" ["name" "age"] age-data)
(mongo/insert-data! mongo/conn "gender" ["name" "gender"] gender-data)
(mongo/insert-data! mongo/conn "follows" ["person_follower" "person_followed"] follows-data)
(mongo/insert-data! mongo/conn "location" ["person" "country" "state" "city"] location-data)


(?<- (stdout) [?id ?name] (jdbc/person ?id ?name))
(?<- (stdout) [?id ?name] (mongo/person ?id ?name))


(?<- (stdout) [?person] (jdbc/age ?person 25))
(?<- (stdout) [?person] (mongo/age ?person 25))


(?<- (stdout) [?person] (jdbc/age ?person ?age) (< ?age 30))
(?<- (stdout) [?person] (mongo/age ?person ?age) (< ?age 30))



(?<- (stdout) [?person ?age] (jdbc/age ?person ?age) (< ?age 30))
(?<- (stdout) [?person ?age] (mongo/age ?person ?age) (< ?age 30))



(?<- (stdout) [?person] (jdbc/follows "emily" ?person) (jdbc/gender ?person "m"))
(?<- (stdout) [?person] (mongo/follows "emily" ?person) (mongo/gender ?person "m"))



(?<- (stdout) [?person ?a2] (jdbc/age ?person ?age) (< ?age 30) (* 2 ?age :> ?a2))
(?<- (stdout) [?person ?a2] (mongo/age ?person ?age) (< ?age 30) (* 2 ?age :> ?a2))



(?<- (stdout) [?person1 ?person2]
  (jdbc/age ?person1 ?age1) (jdbc/follows ?person1 ?person2) (jdbc/age ?person2 ?age2) (< ?age2 ?age1))
(?<- (stdout) [?person1 ?person2]
  (mongo/age ?person1 ?age1) (mongo/follows ?person1 ?person2) (mongo/age ?person2 ?age2) (< ?age2 ?age1))
(?<- (stdout) [?person1 ?person2]
  (mongo/age ?person1 ?age1) (jdbc/follows ?person1 ?person2) (mongo/age ?person2 ?age2) (< ?age2 ?age1))



(?<- (stdout) [?person1 ?person2 ?delta]
  (jdbc/age ?person1 ?age1) (jdbc/follows ?person1 ?person2) (jdbc/age ?person2 ?age2) (- ?age2 ?age1 :> ?delta) (< ?delta 0))
(?<- (stdout) [?person1 ?person2 ?delta]
  (mongo/age ?person1 ?age1) (mongo/follows ?person1 ?person2) (mongo/age ?person2 ?age2) (- ?age2 ?age1 :> ?delta) (< ?delta 0))
(?<- (stdout) [?person1 ?person2 ?delta]
  (mongo/age ?person1 ?age1) (jdbc/follows ?person1 ?person2) (mongo/age ?person2 ?age2) (- ?age2 ?age1 :> ?delta) (< ?delta 0))



(?<- (stdout) [?count] (jdbc/age _ ?a) (< ?a 30) (c/count ?count))
(?<- (stdout) [?count] (mongo/age _ ?a) (< ?a 30) (c/count ?count))



(?<- (stdout) [?person ?count] (jdbc/follows ?person _) (c/count ?count))
(?<- (stdout) [?person ?count] (mongo/follows ?person _) (c/count ?count))



(?<- (stdout) [?country ?avg]
  (jdbc/location ?person ?country _ _) (jdbc/age ?person ?age) (c/count ?count) (c/sum ?age :> ?sum) (div ?sum ?count :> ?avg))
(?<- (stdout) [?country ?avg]
  (mongo/location ?person ?country _ _) (mongo/age ?person ?age) (c/count ?count) (c/sum ?age :> ?sum) (div ?sum ?count :> ?avg))
(?<- (stdout) [?country ?avg]
  (mongo/location ?person ?country _ _) (jdbc/age ?person ?age) (c/count ?count) (c/sum ?age :> ?sum) (div ?sum ?count :> ?avg))




