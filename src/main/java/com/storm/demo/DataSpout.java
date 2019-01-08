package com.storm.demo;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;


public class DataSpout implements IRichSpout {
    private static final long serialVersionUID = -620768344883063619L;
    private static Logger LOG = Logger.getLogger("DataSoupt");
    private SpoutOutputCollector collector;
    private  Queue queues = new LinkedList();
    private Map conf;
    private TopologyContext context;
    private static List list;

    public DataSpout(List list) {
        this.list = list;
    }
    @Override
    public void open(Map conf, TopologyContext context,
                     SpoutOutputCollector collector) {
        this.collector = collector;
        this.conf = conf;
        this.context = context;
        Iterator it = list.iterator();
        while (it.hasNext()) {
            Object accessRecord = it.next();
            queues.add(accessRecord);
        }
    }

    public void close() {
    }
@Override
    public void nextTuple() {
       String accessRecord = (String) queues.poll();
        if(accessRecord!=null){
            this.collector.emit(new Values(new Object[] { accessRecord.split(" ")[0]}));
        }
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(new String[] { "ip" }));
    }

    public void activate() {
    }

    public void deactivate() {
    }




    public Map getComponentConfiguration() {
        return null;
    }
}



