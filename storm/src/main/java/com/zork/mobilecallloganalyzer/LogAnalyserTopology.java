package com.zork.mobilecallloganalyzer;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

/**
 * Author: xiesen
 * Description: 电话记录日志分析Topology
 * Date: Created in 10:54 2018/2/13
 */
public class LogAnalyserTopology {
    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
        config.setDebug(false);
        Map<String,String> map = new HashMap<String, String>();
        map.put("storm.zookeeper.servers","master");
        config.setEnvironment(map);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout());
        builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt(),2).shuffleGrouping("call-log-reader-spout");
        builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt(),2).fieldsGrouping("call-log-creator-bolt", new Fields("call"));

        // 本地模式
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LogAnalyserStorm",config,builder.createTopology());

        // 集群模式
        /*config.put(Config.STORM_CLUSTER_MODE,"distributed");
        StormSubmitter.submitTopology("LogAnalyserStorm",config,builder.createTopology());*/
    }
}