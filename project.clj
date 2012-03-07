(defproject playground "1.0.0-SNAPSHOT"
  :description "demo of cascalog stuff"
  :url "https://github.com/jeffsack/playground"
  :repositories {"conjars" "http://conjars.org/repo/"
                 "sonatype" "http://oss.sonatype.org/content/repositories/releases/"
                 "maven-central" "http://repo1.maven.org/maven2/"}
  :dependencies [[org.clojure/clojure "1.3.0"]
                 [org.clojure/java.jdbc "0.1.1"]
                 [cascalog "1.8.6"]

                 [r0man/cascading.jdbc "1.2"]
                 [backtype/cascading-dbmigrate "1.0.2"]
                 [com.h2database/h2 "1.3.163"]
                 [org.springframework/spring-jdbc "3.1.0.RELEASE"]

                 [org.mongodb/mongo-java-driver "2.7.3"]
                 [org.mongodb/mongo-hadoop-core_cdh3u3 "1.0.0-cascading-rc1-SNAPSHOT"]
;                 [org.mongodb/mongo-hadoop_cdh3u3 "1.0.0-rc0"]
                 [djktno/cascading.mongodb "0.0.50-SNAPSHOT"]
                 [congomongo "0.1.8"]

                 [org.elasticsearch/elasticsearch "0.18.7"]
                 [esperanto/esperanto "1.0.0-SNAPSHOT"]]
  :dev-dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]])
