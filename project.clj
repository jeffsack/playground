(defproject playground "1.0.0-SNAPSHOT"
  :description "demo of cascalog stuff"
  :url "https://github.com/jeffsack/playground"
  :repositories {"conjars" "http://conjars.org/repo/"}
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/java.jdbc "0.1.1"]
                 [cascalog "1.8.5"]
                 [r0man/cascading.jdbc "1.2"]
                 [com.h2database/h2 "1.3.163"]
                 [org.springframework/spring-jdbc "3.1.0.RELEASE"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]])
