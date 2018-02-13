package com.zork.kafka;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import com.zork.randomdataprint.PrintBolt;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;

/**
 * Author: xiesen
 * Description:
 * Date: Created in 14:55 2018/2/12
 */
public class KafkaTopology {
    private static TopologyBuilder builder = new TopologyBuilder();

    static BrokerHosts brokerHosts = new ZkHosts("master:2181,slave1:2181,slave2:2181");
    static String TopologyJobName = "JstormKafka";
    static String topics = "test";
    static long startOffsetTime = -1;
    static String groupid = "jstorm-kafka";
    static String zkRoot = "/jstorm";

    public static void main(String[] args) {
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topics, zkRoot, topics + "_" + groupid);

        Config config = new Config();
        builder.setSpout("KafkaSpout", (IRichSpout) new KafkaSpout(spoutConfig));
        builder.setBolt("PrintBolt", new PrintBolt()).shuffleGrouping("KafkaSpout");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test-KafkaTopology", config, builder.createTopology());

        new KafkaSpout(spoutConfig);
    }
}
