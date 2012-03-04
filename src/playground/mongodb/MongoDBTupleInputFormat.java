package playground.mongodb;

import com.mongodb.*;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;
import org.bson.types.ObjectId;
import playground.ObjectHolder;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class MongoDBTupleInputFormat implements InputFormat<ObjectHolder<ObjectId>, Map<String, Object>> {

    static final Logger log = Logger.getLogger(MongoDBSourceTap.class);

    public InputSplit[] getSplits(JobConf jc, int i) throws IOException {
        DBCollection dbCollection = getDBCollection(jc);
        return new InputSplit[]{new TupleInputSplit(dbCollection.count())};
    }

    public RecordReader<ObjectHolder<ObjectId>, Map<String, Object>> getRecordReader(InputSplit is, JobConf jc, Reporter rprtr) throws IOException {
        DBCollection dbCollection = getDBCollection(jc);
        return new TupleRecordReader(dbCollection);
    }

    public static class TupleInputSplit implements InputSplit {
        public long numObjects;

        public TupleInputSplit() {
        }

        public TupleInputSplit(long numObjects) {
            log.info("building InputSplit of size: " + numObjects);
            this.numObjects = numObjects;
        }

        public long getLength() throws IOException {
            return numObjects;
        }

        public String[] getLocations() throws IOException {
            return new String[]{};
        }

        public void write(DataOutput d) throws IOException {
            d.writeLong(numObjects);
        }

        public void readFields(DataInput di) throws IOException {
            numObjects = di.readLong();
        }
    }

    public static class TupleRecordReader implements RecordReader<ObjectHolder<ObjectId>, Map<String, Object>> {

        int pos = 0;
        private DBCursor dbCursor;
        private final long count;

        public TupleRecordReader(DBCollection dbCollection) {
            this.count = dbCollection.count();
            this.dbCursor = dbCollection.find();
        }

        public boolean next(ObjectHolder<ObjectId> k, Map<String, Object> v) throws IOException {
            if (!dbCursor.hasNext()) {
                return false;
            }
            BasicDBObject nextDbObject = (BasicDBObject) dbCursor.next();
            log.info("retrieved next DBObject: " + nextDbObject);

            k.object = nextDbObject.getObjectId("_id");

            v.clear();
            v.putAll(nextDbObject);
            v.remove("_id");

            log.info("pos: " + pos + "; key: " + k.object + "; value: " + v);

            pos++;
            return true;
        }

        public ObjectHolder<ObjectId> createKey() {
            return new ObjectHolder<ObjectId>();
        }

        public Map<String, Object> createValue() {
            return new HashMap<String, Object>();
        }

        public long getPos() throws IOException {
            return pos;
        }

        public void close() throws IOException {
        }

        public float getProgress() throws IOException {
            if (count == 0) {
                return 1;
            }
            return (float) (pos * 1.0 / count);
        }

    }

    public static String getHost(JobConf jc) {
        return jc.get("mongodbTap.host");
    }

    public static void setHost(JobConf jc, String host) {
        jc.set("mongodbTap.host", host);
    }

    public static int getPort(JobConf jc) {
        return jc.getInt("mongodbTap.port", 27017);
    }

    public static void setPort(JobConf jc, int port) {
        jc.setInt("mongodbTap.port", port);
    }

    public static String getDatabase(JobConf jc) {
        return jc.get("mongodbTap.database");
    }

    public static void setDatabase(JobConf jc, String database) {
        jc.set("mongodbTap.database", database);
    }

    public static String getCollection(JobConf jc) {
        return jc.get("mongodbTap.collection");
    }

    public static void setCollection(JobConf jc, String collection) {
        jc.set("mongodbTap.collection", collection);
    }

    private static DBCollection getDBCollection(JobConf jc) throws UnknownHostException {
        String host = getHost(jc);
        int port = getPort(jc);
        String database = getDatabase(jc);
        String collection = getCollection(jc);
        Mongo mongo = new Mongo(host, port);
        DB db = mongo.getDB(database);
        DBCollection dbCollection = db.getCollection(collection);
        log.info("connected to DB: " + db + "; retrieved collection: " + dbCollection);
        return dbCollection;
    }
}