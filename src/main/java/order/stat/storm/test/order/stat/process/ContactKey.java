package order.stat.storm.test.order.stat.process;

import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 *
 * 方便存储  进行key的拼接
 */
public class ContactKey implements Function {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        //  "yyyyMMddHHStr", "price", "country", "provence", "city
        //由于存入habse 一般以city
        collector.emit(new Values(
                tuple.getStringByField("yyyyMMddHHStr") + "_"
                        + tuple.getStringByField("country") + "_"
                        + tuple.getStringByField("provence") + "_"
                        + tuple.getStringByField("city") + "_"));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
