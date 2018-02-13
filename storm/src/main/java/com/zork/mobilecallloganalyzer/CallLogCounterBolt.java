package com.zork.mobilecallloganalyzer;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: xiesen
 * Description: 统计计算电话呼叫次数
 * Date: Created in 10:48 2018/2/13
 */
public class CallLogCounterBolt implements IRichBolt {
    Map<String, Integer> counterMap;
    private OutputCollector collector;

    /**
     *  bolt初始化过程
     * @param stormConf stormbolt配置初始化
     * @param context storm上下文
     * @param collector storm收集器
     */
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.counterMap = new HashMap<String, Integer>();
        this.collector = collector;
    }

    /**
     * 一个元祖的处理过程
     * @param input
     */
    public void execute(Tuple input) {
        String call = input.getString(0);
        Integer duration = input.getInteger(1);
        if (!counterMap.containsKey(call)) {
            counterMap.put(call, 1);
        } else {
            Integer c = counterMap.get(call) + 1;
            counterMap.put(call, c);
        }
        collector.ack(input);
    }

    /**
     * bolt执行结束，调用此方法
     */
    public void cleanup() {
        for (Map.Entry<String, Integer> entry : counterMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    /**
     * 声明元祖的输出模式
     * @param declarer
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("call"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
