package com.storm.wordcount;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * @author rjsong
 */
public class SpoutBolt extends BaseRichSpout {

    SpoutOutputCollector collector;
    HashMap data=null;
    /**
     * 初始化方法
     */
    public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
      data=  new HashMap<>();
      data.put(1,"hello world this is a test");
      data.put(2,"java world this is a test");
      data.put(3,"hive bigdata this is a test");
      data.put(4,"spark pathon this is a test");
    }
    /**
     * 重复调用方法
     */
    public void nextTuple() {
        collector.emit(new Values(data.get(4)));
    }
    /**
     *
     * 输出
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("test"));
    }

}
