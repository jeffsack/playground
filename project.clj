(defproject playground "1.0.0-SNAPSHOT"
  :description "demo of cascalog stuff"
  :url "https://github.com/jeffsack/playground"
  :repositories {"conjars" "http://conjars.org/repo/"}
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [cascalog "1.8.5"]
                 [r0man/cascading.jdbc "1.2"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]])
