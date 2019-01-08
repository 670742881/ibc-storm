package com.storm.demo.fileData;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;

public class MySpout implements IRichSpout {
    SpoutOutputCollector collector;
    FileInputStream fis;
    InputStreamReader isr;
    BufferedReader br;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        try {
            this.fis = new FileInputStream("track.log");
            this.isr = new InputStreamReader(fis, "UTF-8");
            this.br = new BufferedReader(isr);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



String str;
    @Override
    public void nextTuple()  {
        try {
            while ((str = this.br.readLine()) != null){
                //将读到的直接发送过去
   collector.emit(new Values(str));
        // Thread.sleep(1000);

            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void ack(Object msgId) {
        System.out.println("spout ack"+msgId);

    }

    @Override
    public void fail(Object msgId) {
        System.out.println("spout f"+msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("log"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    @Override
    public void close() {
        try {
            br.close();
            isr.close();
            fis.close();
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }


    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }
}
