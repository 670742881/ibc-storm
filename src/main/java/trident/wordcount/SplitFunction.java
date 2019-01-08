package trident.wordcount;

import org.apache.storm.trident.operation.Function;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class SplitFunction implements Function {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
       String log= tuple.getString(0);
       if (log!=null){
         String[] words=  log.split(" ");

             for (String word:words) {
              //   System.err.println(word);
                 collector.emit(new Values(word));

         }
       }
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
