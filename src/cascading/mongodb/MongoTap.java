package cascading.mongodb;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.*;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.ObjectHolder;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.log4j.Logger;
import org.bson.BSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

/**
 * User: jeff
 * Date: 3/6/12
 * Time: 10:51 PM
 */
public class MongoTap extends Tap {

    private String id = UUID.randomUUID().toString();

    public MongoTap(String inputUri, String outputUri, Fields fields) {
        super(new MongoDBScheme(inputUri, outputUri, fields));
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
    public Fields getSinkFields() {
        throw new UnsupportedOperationException("unable to sink tuple streams via a SourceTap instance");
    }

    @Override
    public void sink(TupleEntry tupleEntry, OutputCollector outputCollector) throws IOException {
        throw new UnsupportedOperationException("unable to sink tuple streams via a SourceTap instance");
    }

    @Override
    public final boolean isSink() {
        return false;
    }

    /**
     * @see cascading.tap.Tap#deletePath(org.apache.hadoop.mapred.JobConf)
     */
    public boolean deletePath(JobConf conf) throws IOException {
        throw new UnsupportedOperationException("unable to delete files via a SourceTap instance");
    }

    /**
     * @see cascading.tap.Tap#makeDirs(JobConf)
     */
    public boolean makeDirs(JobConf conf) throws IOException {
        throw new UnsupportedOperationException("unable to make dirs via a SourceTap instance");
    }

    public TupleEntryIterator openForRead(JobConf conf) throws IOException {
        throw new UnsupportedOperationException("unable to open for read via a SourceTap instance");
    }

    public TupleEntryCollector openForWrite(JobConf conf) throws IOException {
        throw new UnsupportedOperationException("unable to open for write via a SourceTap instance");
    }

    public static class MongoDBScheme extends Scheme {

        static final Logger log = Logger.getLogger(MongoDBScheme.class);

        private String inputUri;
        private String outputUri;

        public MongoDBScheme(String inputUri, String outputUri, Fields fields) {
            super(fields);
            this.inputUri = inputUri;
            this.outputUri = outputUri;
        }

        @Override
        public void sourceInit(Tap tap, JobConf conf) throws IOException {
            FileInputFormat.setInputPaths(conf, "/" + UUID.randomUUID().toString());
            conf.setInputFormat(MongoInputFormat.class);

            MongoConfigUtil.setInputURI(conf, inputUri);
            MongoConfigUtil.setOutputURI(conf, outputUri);

//            MongoConfigUtil.setQuery( conf, "{\"x\": {\"$regex\": \"^eliot\", \"$options\": \"\"}}");

        }

        @Override
        public Tuple source(Object k, Object v) {
            log.debug("source(" + k + ", " + v + ")");

            Object key = ((ObjectHolder<Object>) k).object;
            BSONObject value = (BSONObject) v;

            Iterator iterator = getSourceFields().iterator();
            ArrayList<Object> values = new ArrayList<Object>();
            for (; iterator.hasNext(); ) {
                String fieldName = iterator.next().toString();
                values.add(value.get(fieldName));
            }
            Tuple tuple = new Tuple(values.toArray());
            log.info("source() built tuple: " + tuple);
            return tuple;
        }

        @Override
        public void sinkInit(Tap tap, JobConf jc) throws IOException {
            jc.setOutputFormat(MongoOutputFormat.class);
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void sink(TupleEntry te, OutputCollector oc) throws IOException {
            throw new UnsupportedOperationException("Not supported.");
        }

    }

}
