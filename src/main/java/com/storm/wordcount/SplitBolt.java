package com.storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author rjsong
 */
public class SplitBolt extends BaseRichBolt {

    OutputCollector collector;

    /**
     * 初始化
     */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    /**x
     * 执行方法
     */
    public void execute(Tuple input) {
        String line = input.getString(0);
        String[] split = line.split(" ");
        for (String word : split) {
            collector.emit(new Values(word));
        }
    }
    /**
     * 输出
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

}

