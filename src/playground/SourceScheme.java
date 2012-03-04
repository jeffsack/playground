package playground;

import cascading.scheme.Scheme;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;

import java.io.IOException;

/**
 * User: jeff
 * Date: 3/3/12
 * Time: 3:25 PM
 */
public abstract class SourceScheme extends Scheme {
    public SourceScheme(Fields fields) {
        super(fields);
    }

    @Override
    public void sinkInit(Tap tap, JobConf jc) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void sink(TupleEntry te, OutputCollector oc) throws IOException {
        throw new UnsupportedOperationException("Not supported.");
    }
}
