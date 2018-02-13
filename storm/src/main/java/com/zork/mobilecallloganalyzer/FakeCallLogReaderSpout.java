package com.zork.mobilecallloganalyzer;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Author: xiesen
 * Description: 数据输入源
 * Date: Created in 10:26 2018/2/13
 */
public class FakeCallLogReaderSpout implements IRichSpout {
    // 收集器
    private SpoutOutputCollector collector;
    private boolean completed;
    // 上下文环境信息
    private TopologyContext context;
    private Random randomGenerator;
    private Integer idx;

    /**
     *  初始化Spout
     * @param map
     * @param topologyContext
     * @param spoutOutputCollector
     */
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.context = topologyContext;
        this.completed = false;
        this.randomGenerator = new Random();
        this.idx = 0;
    }

    /**
     * 通过收集器发送生成的数据
     */
    public void nextTuple() {
        Utils.sleep(1000);
        List<String> mobileNumbers = new ArrayList<String>();
        mobileNumbers.add("18503816311");
        mobileNumbers.add("18503816312");
        mobileNumbers.add("18503816313");
        mobileNumbers.add("18503816314");

        String fromMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
        String toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));

        while (fromMobileNumber == toMobileNumber) {
            toMobileNumber = mobileNumbers.get(randomGenerator.nextInt(4));
        }

        Integer duration = randomGenerator.nextInt(60);
        System.out.println(fromMobileNumber + " - " + toMobileNumber + " , " + duration);
        this.collector.emit(new Values(fromMobileNumber, toMobileNumber, duration));
    }


    public void close() {
    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void ack(Object o) {

    }

    public void fail(Object o) {

    }

    /**
     * 声明元组的输出模式
     * @param outputFieldsDeclarer
     */
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("from", "to", "duration"));
    }

    public boolean isDistributed() {
        return false;
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
