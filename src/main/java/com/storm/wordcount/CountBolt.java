package com.storm.wordcount;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * @author rjsong
 */
public class CountBolt extends BaseRichBolt {
    OutputCollector collector;
    Map<String, Integer> map = new HashMap<String, Integer>();
    /**
     * 初始化
     */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    /**
     * 执行方法
     */
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        if(map.containsKey(word)){
            Integer c = map.get(word);
            map.put(word, c+1);
        }else{
            map.put(word, 1);
        }
        //测试输出
        System.out.println("结果:"+map);
    }
    /**
     * 输出
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}
