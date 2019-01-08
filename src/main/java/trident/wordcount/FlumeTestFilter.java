package trident.wordcount;


import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Map;

public class FlumeTestFilter implements Filter {

    @Override
    public boolean isKeep(TridentTuple tuple) {
      String str= tuple.getString(0);
      //打印测试

        //做过率判断
    if (str.contains("flume")) {
        System.err.println("&&&&"+str);
        return true;
    }
    else
        return false;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {

    }

    @Override
    public void cleanup() {

    }
}
