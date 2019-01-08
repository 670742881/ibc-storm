package com.storm.demo;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.InputDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;





    public class CountTopology {
        private static ArrayList zkServers = new ArrayList();
        private static String zkStrings = null;
        private static List list =  new ArrayList();
        public static void main(String[] args) throws Exception {
            String file = "E:/hadoop-workspace/storm/data/data.txt";
            initData(file);
            TopologyBuilder builder = new TopologyBuilder();
            builder.setSpout("1", new DataSpout(list), Integer.valueOf(1));
            InputDeclarer fieldsGrouping = builder.setBolt("2", new SplitBolt(), Integer.valueOf(1));
            fieldsGrouping.fieldsGrouping("1", new Fields(new String[] { "ip" }));
           // builder.setBolt("3", new CountBolt(), Integer.valueOf(5)).fieldsGrouping("2", new Fields(new String[] { "ip" }));
            Config conf = new Config();
            conf.setDebug(false);

            conf.setNumWorkers(20);
            conf.setMaxSpoutPending(5000);

            zkServers.add("ip1");
            zkServers.add("ip2");
            zkServers.add("ip3");
            zkStrings = "ip1,ip2,ip3";
            try
            {
                conf.put(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT, Integer.valueOf(30000));
                conf.put(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT, Integer.valueOf(30000));
                conf.put(Config.STORM_ZOOKEEPER_RETRY_TIMES, Integer.valueOf(10));
                conf.setNumWorkers(1);
                conf.setNumAckers(0);
                conf.put(Config.NIMBUS_HOST, "ip1");
                conf.put(Config.NIMBUS_THRIFT_PORT, Integer.valueOf(6627));
                conf.put(Config.STORM_ZOOKEEPER_PORT, Integer.valueOf(2181));
                conf.put(Config.STORM_ZOOKEEPER_SERVERS, zkServers);

                //远程模式
//    StormSubmitter.submitTopology("wordcount-topology-demo", conf, buildTopology(args[0]));
                //本地模式
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("wordcount-topology-demo", conf, builder.createTopology());
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }


        private static void initData(String filePath) throws Exception {
            FileReader fr = new FileReader(filePath);
            BufferedReader br = new BufferedReader(fr);
            String line = null;
            while((line = br.readLine()) != null){
                list.add(line);
            }
        }

    }

