package trident.wordcount;


import org.apache.storm.trident.operation.Filter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

public class PrintTestFilter2 implements Filter {

    /**
     *
     */
    private static final long serialVersionUID = 6868959143417503368L;

    private int partitionIndex;

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        // TODO Auto-generated method stub
        this.partitionIndex = context.getPartitionIndex();
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }
    /**
     * 实现是否将Tuple保留在Stream中的逻辑
     */
    @Override
    public boolean isKeep(TridentTuple tuple) {

        List<Object> values = tuple.getValues();
        StringBuilder sbuilder = new StringBuilder(" ");

        int i = 0;
        for(Object value : values){

            if(i == 0){
                sbuilder.append(value);
            }else{
                sbuilder.append("," + value);
            }

            i++;
        }

        System.err.println(sbuilder.toString());
        return true;
    }

}
