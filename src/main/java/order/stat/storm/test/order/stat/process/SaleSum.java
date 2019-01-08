package order.stat.storm.test.order.stat.process;

import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;

import java.util.Map;

//进行同一批次各个分区内的局部统计

public class SaleSum implements Aggregator<SaleSumState> {
    private Logger logger  = org.slf4j.LoggerFactory.getLogger(SaleSum.class);


    /**
     *
     */
    private static final long serialVersionUID = -6879728480425771684L;

    private int partitionIndex ;
    @Override
    public SaleSumState init(Object batchId, TridentCollector collector) {
        return new SaleSumState();

    }

    @Override
    public void aggregate(SaleSumState val, TridentTuple tuple, TridentCollector collector) {
      double oldSum=val.saleSum;
      double price=tuple.getDoubleByField("price");
      double newSum=oldSum+price;
      val.saleSum=newSum;
      }



    @Override
    public void complete(SaleSumState val, TridentCollector collector) {
        collector.emit(new Values(val.saleSum));
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
