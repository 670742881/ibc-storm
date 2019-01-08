package trident.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.Aggregator;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.testing.LRUMemoryMapState;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.testing.Split;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class WordCountTop {
    public static void main(String[] args) {
        //创建top
        TridentTopology WCTopology = new TridentTopology();
        //构建流
        //框架里的spout用于测试
        FixedBatchSpout testSpout = new FixedBatchSpout(new Fields("log", "describle"), 3,
                new Values("hadoop hive spark fulem", "hadoop is bigdata basic"),
                new Values("hadoop hive spark fulem", "hive is compute art"),
                new Values("hadoop hive spark fulem", " sssss"),
                new Values("hadoop hive spark fulem", "s1`sss12 edd"),
                new Values("word count hadoop hive spark", "hadoop is bigdata basic"),
                new Values("word flume hadoop hive spark", "hive is compute art"),
                new Values("word count hadoop hive spark", " sssss"),
                new Values("word count hadoop hive spark", "s1`sss12 edd"));
        testSpout.setCycle(false);
        //构造DAg 流 指定数据采集器
        WCTopology.newStream("SPOUT_ID", testSpout)
                //指定tuple中哪些keyvaue 进行过滤操作
           //     .each(new Fields("log"), new FlumeTestFilter())
              //  .each(new Fields("log", "describle"), new PrintTestFilter2())
                //对tuple中log进行切分
                //top中新增的keyvalue 追加到tuple的后面
                .each(new Fields("log"), new SplitFunction(),new Fields("word"))
                //设置两个task线程进行split
                .parallelismHint(2)
                //进行投影只保留 已word的为key的值
                .project(new Fields("word"))
             //  .partitionBy(new Fields("word"))
                .groupBy(new Fields("word"))
                .chainedAgg()
                //同一批次分区内每个   单词聚合统计  次数
                .aggregate(new Fields("word"), new AggregatorCount(),new Fields("count"))
                //.each(new Fields("word"),new WordCountFunction(),new Fields("")).toStream()
                .chainEnd()
                .parallelismHint(3)
                //每个单词 全局的统计 的最终的状态保存 和更新
                .groupBy(new Fields("word"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("count"),new Sum(),new Fields("globalcount"))
                .newValuesStream()
                 .each(new Fields("word","globalcount"),new PrintTestFilter2());

        //本地测试
        LocalCluster localCluster = new LocalCluster();
        Config config = new Config();
        localCluster.submitTopology("wordcount", config, WCTopology.build());
    }
}
