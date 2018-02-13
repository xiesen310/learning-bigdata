package com.zork.wordcount;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

/**
 * Author: xiesen
 * Description:
 * Date: Created in 11:26 2018/2/12
 */
public class WordReader implements IRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private FileReader fileReader;
    private boolean completed = false;
    private TopologyContext topologyContext;

    public boolean isDistributed() {
        return false;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.spoutOutputCollector = collector;
        try {
            this.fileReader = new FileReader(conf.get("wordsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file [" + conf.get("wordFile") + "]");
        }
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        if (completed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            return;
        }

        String str;
        BufferedReader reader = new BufferedReader(fileReader);
        try {
            while ((str = reader.readLine()) != null) {
                this.spoutOutputCollector.emit(new Values(str), str);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        } finally {
            this.completed = true;
        }
    }

    public void ack(Object msgId) {
        System.out.println("OK: " + msgId);
    }

    public void fail(Object msgId) {
        System.out.println("FAIL: " + msgId);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
