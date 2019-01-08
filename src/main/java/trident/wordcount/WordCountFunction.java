package trident.wordcount;


import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.HashMap;
import java.util.Map;

public class WordCountFunction implements Function {
    HashMap<String,Integer> map=new HashMap<>();
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
       String word= tuple.getString(0);
       Integer count=0;
     if (map.containsKey(word)){
         count=map.get(word);
     }
     count++;
     map.put(word,count);
        System.err.println("partitionIndex"+partitionIndex+"word：="+word+"  count：="+count);
    }
   int partitionIndex;
    @Override
    public void prepare(Map conf, TridentOperationContext context) {
     partitionIndex=  context.getPartitionIndex();
        System.err.println("countFunction的分区编号"+partitionIndex);

    }

    @Override
    public void cleanup() {

    }
}
