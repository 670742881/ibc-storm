package order.stat.storm.test.order.stat.process;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hbase.trident.mapper.SimpleTridentHBaseMapMapper;
import org.apache.storm.hbase.trident.mapper.TridentHBaseMapMapper;
import org.apache.storm.hbase.trident.state.HBaseMapState;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.FilterNull;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.operation.builtin.Sum;
import org.apache.storm.trident.state.OpaqueValue;
import org.apache.storm.trident.state.StateFactory;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import trident.wordcount.PrintTestFilter2;

import java.util.Random;

//订单处理主程序入口
public class OderProcessTrident {
    public static final String SPOUT_ID = "kafak_spout";

    public static void main(String[] args) {
        TridentTopology topology = new TridentTopology();
        //1.从kafak读取数据，
        //只会被成功处理 一次 ，有且只有此一次 提供容错机制  处理失败会在后续的批次进行提交
        BrokerHosts zkHost = new ZkHosts("hadoop01:2181,hadoop02:2181,hadoop03:2181");
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(zkHost, "test");//两种构造器

        kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        //透明事务kafka的spout
        OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
        //严格模式的事务级别
        TransactionalTridentKafkaSpout kafkaSpout1 = new TransactionalTridentKafkaSpout(kafkaConfig);
        //普通的kafak级别 {"str","msg"}
        //严格的kafak级别 {"str","msg"}
        Stream stream = topology.newStream(SPOUT_ID, kafkaSpout);
     // stream.each(new Fields("str"),new PrintTestFilter2());
        Stream hasPraseSteam = stream.each(new Fields("str"), new ParseFunction(), new Fields("timeStamp", "yyyyMMddStr", "yyyyMMddHHStr", "yyyyMMddHHmmStr", "consumer", "productNmae", "price", "country", "provence", "city"));
              //  .each(new Fields("str", "timeStamp", "yyyyMMddStr", "yyyyMMddHHStr", "yyyyMMddHHmmStr", "consumer", "productNmae", "price", "country", "provence", "city"), new PrintTestFilter2());
//
        //1. 对每天电商的销售额
        //去掉用不到的自地段 保留需要用到的字段
        //分区统计的流
        Stream partitionStatStream = hasPraseSteam.project(new Fields("yyyyMMddStr", "price"))
                .shuffle()
                .groupBy(new Fields("yyyyMMddStr"))
                .chainedAgg()
                .partitionAggregate(new Fields("price"), new SaleSum(), new Fields("saleTotalpartByDay")) //进行同一批次各个分区的局部销售额统计
                .partitionAggregate(new Fields("price"), new Count(), new Fields("oderNumOfpartDay"))//同一批次中各个分区的订单数
                .chainEnd()
                .toStream()
                .parallelismHint(2);

        //全局统计 每天的总销售额
        TridentState saleGlobalState = partitionStatStream.groupBy(new Fields("yyyyMMddStr"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("saleTotalpartByDay"), new Sum(), new Fields("saleGlobalAmtDay"));
        //测试
        saleGlobalState.newValuesStream().each(new Fields("yyyyMMddStr", "saleGlobalAmtDay"), new PrintTestFilter2());
        //全局统计 每天的订单总数
        TridentState oderGlobalState = partitionStatStream.groupBy(new Fields("yyyyMMddStr"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("oderNumOfpartDay"), new Sum(), new Fields("oderGlobalAmtDay"));
        oderGlobalState.newValuesStream().each(new Fields("yyyyMMddStr", "oderGlobalAmtDay"), new PrintTestFilter2());


        //2.给与地域时段  维度 统计

        //    "timeStamp","yyyyMMddStr","yyyyMMddHHStr","yyyyMMddHHmmStr","consumer","productNmae","price","country","provence","city"

        TridentState state = hasPraseSteam.project(new Fields("yyyyMMddHHStr", "price", "country", "provence", "city"))
                .each(new Fields("yyyyMMddHHStr", "country", "provence", "city"), new ContactKey(), new Fields("addrAndHour"))
              //  .project()
                .groupBy(new Fields("addrAndHour"))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields("price"), new Sum(), new Fields("saleAmtOfAddrAndHour"));

       //测试
        state.newValuesStream().each(new Fields("addrAndHour"), new PrintTestFilter2());


        //3.使用hbase存入 结果状态
        /**rowkey
         * value
         * 非实物 ：就简单存储一个value
         * 严格的事实控制： 存储: batchId和统计值
         * 透明事务控制 ： batchId和统计值和上个批次的统计值
         */
         HBaseMapState.Options<OpaqueValue> opts=new HBaseMapState.Options<OpaqueValue>();
         opts.tableName="test";
         opts.columnFamily="info";
         //1.1以后设置列名使用下面类
        TridentHBaseMapMapper mapMapper= new SimpleTridentHBaseMapMapper("saleAmtOfAddrAndHour");
        opts.mapMapper = mapMapper;
        StateFactory Hbasefactory=HBaseMapState.opaque(opts);

        //事务类型的
//        HBaseMapState.Options<Object> opts=new HBaseMapState.Options<Object>();
//        opts.tableName="test";
//        opts.columnFamily="info";
//        //1.1以后设置列名使用下面类
//        TridentHBaseMapMapper mapMapper= new SimpleTridentHBaseMapMapper("saleAmtOfAddrAndHour");
//        opts.mapMapper = mapMapper;
//       StateFactory Hbasefactory1=HBaseMapState.nonTransactional(opts);

        TridentState HbaseState = hasPraseSteam.project(new Fields("yyyyMMddHHStr", "price", "country", "provence", "city"))
                .each(new Fields("yyyyMMddHHStr", "country", "provence", "city"), new ContactKey(), new Fields("addrAndHour"))
                //  .project()
                .groupBy(new Fields("addrAndHour"))
                .persistentAggregate(Hbasefactory, new Fields("price"), new Sum(), new Fields("saleAmtOfAddrAndHour"));

        //进行drpc查询
        LocalDRPC localDRPC = new LocalDRPC();
        topology.newDRPCStream("saleAmtOfDay", localDRPC)
                .each(new Fields("args"), new SplitFunction1(), new Fields("requestDate"))
                .stateQuery(saleGlobalState, new Fields("requestDate"), new MapGet(),
                        new Fields("saleGlobalAmtOfDay1"))
                .project(new Fields("requestDate", "saleGlobalAmtOfDay1"))
                .each(new Fields("saleGlobalAmtOfDay1"), new FilterNull())
              //  .each(new Fields("requestDate", "saleGlobalAmtOfDay1"), new PrintTestFilter2())
        ;

        topology.newDRPCStream("numOrderOfDay", localDRPC)
                .each(new Fields("args"), new SplitFunction1(), new Fields("requestDate"))
                .stateQuery(oderGlobalState, new Fields("requestDate"), new MapGet(),
                        new Fields("numOrderGlobalOfDay1"))
                .project(new Fields("requestDate", "numOrderGlobalOfDay1"))
                .each(new Fields("numOrderGlobalOfDay1"), new FilterNull())
        ;


        topology.newDRPCStream("saleTotalAmtOfAddrAndHour", localDRPC)
                .each(new Fields("args"), new SplitFunction1(), new Fields("requestAddrAndHour"))
                .stateQuery(HbaseState, new Fields("requestAddrAndHour"),
                        new MapGet(), new Fields("saleTotalAmtOfAddrAndHour"))
                .project(new Fields("requestAddrAndHour", "saleTotalAmtOfAddrAndHour"))
                .each(new Fields("saleTotalAmtOfAddrAndHour"), new FilterNull())
        ;


        Config conf = new Config();
        if (args == null || args.length <= 0) {
            // 本地测试
            LocalCluster localCluster = new LocalCluster();
            // topology名称唯一
            localCluster.submitTopology("odeR", conf, topology.build());
            while (true) {

                try {
                    Thread.sleep(10000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                String saleAmtResult =
                        localDRPC.execute("saleAmtOfDay", "20160828 20160827");

                System.err.println("saleAmtResult=" + saleAmtResult);

                String numberOrderResult =
                        localDRPC.execute("numOrderOfDay", "20160828 20160827");
                System.err.println("numberOrderResult=" + numberOrderResult);

                String saleTotalAmtOfAddrAndHourRessult =
                        localDRPC.execute("saleTotalAmtOfAddrAndHour", "苏州_江苏_中国_2016082815");

                System.err.println(saleTotalAmtOfAddrAndHourRessult);
            }
        } else {
            try {
                StormSubmitter.submitTopology(args[0], conf, topology.build());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            } catch (AuthorizationException e) {
                e.printStackTrace();
            }
        }
    }
}

