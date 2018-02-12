package com.zork.randomdataprint;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

/**
 * Author: xiesen
 * Description:
 * Date: Created in 13:27 2018/2/12
 */
public class TopologyRandomDataPrint {
    private static TopologyBuilder builder = new TopologyBuilder();

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Config config = new Config();
        builder.setSpout("RandomSentence", new RandomSentenceSpout(), 2);
        builder.setBolt("WordNormalizer", new PrintBolt(), 2).shuffleGrouping("RandomSentence");
        config.setDebug(false);
        config.put(Config.STORM_CLUSTER_MODE,"distributed");
        StormSubmitter.submitTopology("test-random-topology-distributed",config,builder.createTopology());

        /*LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test-random-topology-local", config, builder.createTopology());*/
    }
}
