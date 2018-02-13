package com.zork.mobilecallloganalyzer;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Author: xiesen
 * Description:
 * Date: Created in 10:54 2018/2/13
 */
public class LogAnalyserTopology {
    public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        Config config = new Config();
        config.setDebug(false);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("call-log-reader-spout", new FakeCallLogReaderSpout());
        builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt(),2).shuffleGrouping("call-log-reader-spout");
        builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt(),2).fieldsGrouping("call-log-creator-bolt", new Fields("call"));

        /*LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("LogAnalyserStorm",config,builder.createTopology());*/

        config.put(Config.STORM_CLUSTER_MODE,"distributed");
        StormSubmitter.submitTopology("LogAnalyserStorm",config,builder.createTopology());
    }
}