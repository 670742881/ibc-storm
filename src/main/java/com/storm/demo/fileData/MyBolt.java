package com.storm.demo.fileData;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class MyBolt implements IRichBolt {
    OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector=collector;

    }

    int num = 0;

    @Override
    public void execute(Tuple input) {
        //Object log= input.getValue(0);
        // Object log1=input.getValueByField("log");
        try {
            String log = input.getStringByField("log");
            if (log != null) {
                num++;
                System.err.println("lines:"+num+"---------"+log.split("\t")[1]);
            //    Thread.sleep(1000);
            }
           collector.ack(input);
        } catch (Exception e) {
            collector.fail(input);
            e.printStackTrace();
        }


    }

    @Override
    public void cleanup() {


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(""));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
