//package state;
//
//
//import org.apache.storm.trident.operation.BaseFunction;
//import org.apache.storm.trident.operation.TridentCollector;
//import org.apache.storm.trident.tuple.TridentTuple;
//import org.apache.storm.tuple.Values;
//
//public class MySplit extends BaseFunction {
//    String patton = null;
//
//    public MySplit(String patton) {
//        this.patton = patton;
//    }
//
//    @Override
//    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
//        String log = tridentTuple.getString(0);
//        if (log.length() == 3) {
//            String sessionId = log.split(patton)[1];
//            //格式化日期
//            String date = DateFmt.getCountDate(log.split(patton)[2], DateFmt.date_short);
//            System.err.println(date);
//
//            tridentCollector.emit(new Values(date,"info","pv_count",sessionId));
//        }
//
//
//    }
//}
