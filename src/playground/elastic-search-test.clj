
;;;;;;;;;;;; elastic search stuff

(def settings-builder (org.elasticsearch.common.settings.ImmutableSettings/settingsBuilder))

(defn create-node [settings-builder cluster-name]
  (let [node-builder (org.elasticsearch.node.NodeBuilder/nodeBuilder)]
    (.settings node-builder settings-builder)
    (.clusterName node-builder cluster-name)
    ;            (.local node-builder true)
    (.build node-builder)))

;(.setClusterName  "test-cluster"
;(def node (.build (.settings (org.elasticsearch.node.NodeBuilder/nodeBuilder) settings-builder)))

(defn create-client [node] (.client node))

(def test-node (create-node settings-builder "test-cluser"))
(def test-client (create-client test-node))

(def index "twitter")

(def doc {:type "tweet"
          :text "The quick brown fox jumps over the lazy dog"})

(index-doc test-client index doc)

;import static org.elasticsearch.index.query.FilterBuilders.*;
;import static org.elasticsearch.index.query.QueryBuilders.*;
;
;QueryBuilder qb1 = termQuery("name", "kimchy");

(def qb1 (org.elasticsearch.index.query.QueryBuilders/termQuery "name" "kimchy"))

;
;QueryBuilder qb2 = boolQuery()
;.must(termQuery("content", "test1"))
;.must(termQuery("content", "test4"))
;.mustNot(termQuery("content", "test2"))
;.should(termQuery("content", "test3"));
;
;QueryBuilder qb3 = filteredQuery(
;                                  termQuery("name.first", "shay"),
;                                  rangeFilter("age")
;                                  .from(23)
;                                  .to(54)
;                                  .includeLower(true)
;                                  .includeUpper(false)
;                                  );

; TODO: clean this up and reorganize a bit better, move appropriate stuff into test dir

(def node (make-test-node {}))

(def client (.client node))

(def index "twitter")

(def mapping {:tweet
              {:_source {:enabled true}
               :properties
               {:text
                {:store "yes"
                 :type "string"
                 :index "analyzed"}}}})

(def doc {:type "tweet"
          :text "The quick brown fox jumps over the lazy dog"})

;(defn doc-seq [n]
;  (for [i (range n)]
;    (doto (java.util.HashMap.)
;      (.put "id" (str i))
;      (.put "type" "tweet")
;      (.put "text" "The quick brown fox jumps over the lazy dog"))))
;
;(defn doc-seq [n]
;  (for [i (range n)]
;    (merge doc {:id (str i)})))
;
;(use-fixtures :once (node-fixture node))
;(use-fixtures :each (index-fixture node index mapping))


;(index-doc client index doc)