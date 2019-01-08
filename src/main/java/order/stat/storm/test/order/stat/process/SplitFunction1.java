package order.stat.storm.test.order.stat.process;

import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class SplitFunction1 implements Function {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5709475789218632706L;

	private int partitionIndex;
	@Override
	public void prepare(Map conf, TridentOperationContext context) {
		// TODO Auto-generated method stub
		partitionIndex = context.getPartitionIndex();
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		
		String str = tuple.getStringByField("args");
		
		System.err.println("****** partitionIndex=" +this.partitionIndex +" , str=" + str);
		String[] words = str.split(" ");
		
		for(String word : words){
			collector.emit(new Values(word));
		}
		
	}

}
