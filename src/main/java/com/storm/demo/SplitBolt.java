package com.storm.demo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;



public class SplitBolt implements IRichBolt {
    private static final long serialVersionUID = -424523368294777576L;
    OutputCollector collector;
   @Override
    public void prepare(Map conf, TopologyContext context,
                        OutputCollector collector) {
        this.collector = collector;
    }
@Override
    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);

        for (String word : sentence.split(" ")) {
            this.collector.emit(tuple, new Values(new Object[] { "ip"}));
        }

        this.collector.ack(tuple);
    }

    public void cleanup() {
    }
@Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(new String[] { "word" }));
    }

    public Map getComponentConfiguration() {
        return null;
    }
}
