package playground.mongodb;

import cascading.tap.SourceTap;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import playground.ObjectHolder;
import playground.SourceScheme;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;

public class MongoDBSourceTap extends SourceTap {

    static final Logger log = Logger.getLogger(MongoDBSourceTap.class);

    private String id = UUID.randomUUID().toString();

    public MongoDBSourceTap(String host, int port, String database, String collection, Fields fields) {
        super(new MongoDBSourceScheme(host, port, database, collection, fields));
    }

    @Override
    public Path getPath() {
        return new Path("/" + id);
    }

    @Override
    public boolean pathExists(JobConf jc) throws IOException {
        return true;
    }

    @Override
    public long getPathModified(JobConf jc) throws IOException {
        return System.currentTimeMillis();
    }

    @Override
    public boolean equals(Object object) {
        if (!getClass().equals(object.getClass())) {
            return false;
        }
        MongoDBSourceTap other = (MongoDBSourceTap) object;
        return id.equals(other.id);
    }

    public static class MongoDBSourceScheme extends SourceScheme {

        private String host, database, collection;
        private int port;

        public MongoDBSourceScheme(String host, int port, String database, String collection, Fields fields) {
            super(fields);
            this.host = host;
            this.database = database;
            this.port = port;
            this.collection = collection;
        }

        @Override
        public void sourceInit(Tap tap, JobConf jc) throws IOException {
            FileInputFormat.setInputPaths(jc, "/" + UUID.randomUUID().toString());
            jc.setInputFormat(MongoDBTupleInputFormat.class);
            MongoDBTupleInputFormat.setHost(jc, host);
            MongoDBTupleInputFormat.setDatabase(jc, database);
            MongoDBTupleInputFormat.setPort(jc, port);
            MongoDBTupleInputFormat.setCollection(jc, collection);
        }

        @Override
        public Tuple source(Object key, Object value) {
            log.debug("source(" + key + ", " + value + ")");

            ObjectId objectId = ((ObjectHolder<ObjectId>) key).object;
            Map<String, Object> map = (Map<String, Object>) value;

            Iterator iterator = getSourceFields().iterator();
            ArrayList<Object> values = new ArrayList<Object>();
            for (; iterator.hasNext(); ) {
                String fieldName = iterator.next().toString();
                if (fieldName.equals("_id")) {
                    values.add(objectId.toString());
                } else {
                    values.add(map.get(fieldName));
                }
            }
            Tuple tuple = new Tuple(values.toArray());
            log.info("source() built tuple: " + tuple);
            return tuple;
        }

    }

}
