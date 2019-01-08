package com.storm.wordcount;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * @author rjsong
 */
public class WordCount {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        //创建一个TopologyBuilder
        TopologyBuilder tb = new TopologyBuilder();
        tb.setSpout("SpoutBolt", new SpoutBolt());
        tb.setBolt("SplitBolt", new SplitBolt()).shuffleGrouping("SpoutBolt");
        tb.setBolt("CountBolt", new CountBolt()).fieldsGrouping("SplitBolt", new Fields("word"));
        //创建配置
        Config conf = new Config();
        //设置worker数量
        conf.setNumWorkers(2);
        //提交任务
        //集群提交
        //      StormSubmitter.submitTopology("myWordcount", conf, tb.createTopology());
        //本地提交
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("myWordcount", conf, tb.createTopology());

    }
}

