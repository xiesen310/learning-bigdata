package com.zork.wordcount;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Author: xiesen
 * Description:
 * Date: Created in 11:48 2018/2/12
 */
public class TopologyWordCount {

    public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new WordReader());
        builder.setBolt("word-normalizer", new WordNormalize()).shuffleGrouping("word-readers");
        builder.setBolt("word-counter", new WordCounter(), 2).fieldsGrouping("word-normalizer", new Fields("word"));

        Config config = new Config();
        config.put("wordsFile", args[0]);
        config.setDebug(false);
        config.put(Config.STORM_CLUSTER_MODE,"distributed");
        StormSubmitter.submitTopology("Getting-Started-Toplogie",config,builder.createTopology());

        /*LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", config, builder.createTopology());*/
    }
}
