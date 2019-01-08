package trident.wordcount;

import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * 同一批次内各个分区的分组统计
 */
public class AggregatorCount implements Aggregator<AggregatorCount.CountState> {
    @Override
    public CountState init(Object batchId, TridentCollector collector) {
        return new CountState();
    }

    @Override
    public void aggregate(CountState val, TridentTuple tuple, TridentCollector collector) {
        int oldCount = val.count;
        int newCount = oldCount + 1;
        val.count = newCount;
    }

    @Override
    public void complete(CountState val, TridentCollector collector) {
        collector.emit(new Values(val.count));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }

    class CountState {
        int count;
    }

}
