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


(?<- (stdout) [?id ?name] (jdbc/person ?id ?name))

(?<- (stdout) [?id ?name] (mongo/person ?id ?name))


(?<- (stdout) [?person] (jdbc/age ?person 25))

(?<- (stdout) [?person] (mongo/age ?person 25))


(?<- (stdout) [?person] (jdbc/age ?person ?age) (< ?age 30))
(?<- (stdout) [?person] (mongo/age ?person ?age) (< ?age 30))

(?<- (stdout) [?person ?age] (jdbc/age ?person ?age)
         (< ?age 30))

(?<- (stdout) [?person] (jdbc/follows "emily" ?person)
         (gender ?person "m"))

(?<- (stdout) [?person ?a2] (jdbc/age ?person ?age)
  (< ?age 30) (* 2 ?age :> ?a2))

(?<- (stdout) [?person1 ?person2]
  (jdbc/age ?person1 ?age1) (jdbc/follows ?person1 ?person2)
  (jdbc/age ?person2 ?age2) (< ?age2 ?age1))

(?<- (stdout) [?person1 ?person2]
  (jdbc/age ?person1 ?age1) (mongo/follows ?person1 ?person2)
  (jdbc/age ?person2 ?age2) (< ?age2 ?age1))


(?<- (stdout) [?person1 ?person2 ?delta]
  (jdbc/age ?person1 ?age1) (jdbc/follows ?person1 ?person2)
  (jdbc/age ?person2 ?age2) (- ?age2 ?age1 :> ?delta)
  (< ?delta 0))

(?<- (stdout) [?count] (jdbc/age _ ?a) (< ?a 30)
  (c/count ?count))

(?<- (stdout) [?person ?count] (jdbc/follows ?person _)
  (c/count ?count))

(?<- (stdout) [?country ?avg]
  (jdbc/location ?person ?country _ _) (jdbc/age ?person ?age)
  (c/count ?count) (c/sum ?age :> ?sum)
  (div ?sum ?count :> ?avg))



