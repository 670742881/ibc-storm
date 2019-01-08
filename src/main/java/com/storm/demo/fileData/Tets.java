package com.storm.demo.fileData;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class Tets {
    public static void main(String[] args) {
  TopologyBuilder st=new TopologyBuilder();
        st.setSpout("spout",new MySpout(),1);
        st.setBolt("bolt",new MyBolt(),1).shuffleGrouping("spout");
     Config conf=new Config();
     conf.setNumWorkers(1);
        LocalCluster cluster=new LocalCluster();
        cluster.submitTopology("wordcount-topology-demo", conf, st.createTopology());
    }
}
