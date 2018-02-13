package com.zork.wordcount;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: xiesen
 * Description:
 * Date: Created in 11:41 2018/2/12
 */
public class WordCounter implements IRichBolt {
    Integer id;
    String name;
    Map<String, Integer> counters;
    private OutputCollector outputCollector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        this.counters = new HashMap<String, Integer>();
        this.id = context.getThisTaskId();
        this.name = context.getThisComponentId();
    }

    public void execute(Tuple input) {

        String str = input.getString(0);
        if (!counters.containsKey(str)) {
            counters.put(str, 1);
        } else {
            Integer c = counters.get(str) + 1;
            counters.put(str, c);
        }
        outputCollector.ack(input);
    }

    public void cleanup() {
        System.out.println("--- Word counter [" + name + "-" + id + "] ---");
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
