package com.zork.mobilecallloganalyzer;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Author: xiesen
 * Description: 创建电话记录的执行器
 * Date: Created in 10:44 2018/2/13
 */
public class CallLogCreatorBolt implements IRichBolt {
    private OutputCollector collector;

    /**
     * 初始化Bolt
     * @param stormConf storm配置
     * @param context storm上下文
     * @param collector 输出执行器
     */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    /**
     * 元组的输入过程
     * @param input
     */
    public void execute(Tuple input) {
        String from = input.getString(0);
        String to = input.getString(1);
        Integer duration = input.getInteger(2);
        collector.emit(new Values(from + " - " + to, duration));
    }

    /**
     * bolt 结束的时候调用，关闭资源
     */
    public void cleanup() {

    }

    /**
     *  声明元组的输出模式
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("call", "duration"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
