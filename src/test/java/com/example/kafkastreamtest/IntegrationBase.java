package com.example.kafkastreamtest;

import kafka.server.KafkaConfig$;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig;
import net.mguenther.kafka.junit.EmbeddedKafkaConfig;

public class IntegrationBase {
    public static EmbeddedKafkaCluster embeddedKafkaCluster;
    static {
        embeddedKafkaCluster =
                EmbeddedKafkaCluster.provisionWith(EmbeddedKafkaClusterConfig.create()
                        .provisionWith(EmbeddedKafkaConfig.create()
                                .with(KafkaConfig$.MODULE$.LogDirsProp(), "/Users/lap01171/Desktop/kafka/")
                                .build())
                        .build());
        embeddedKafkaCluster.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                embeddedKafkaCluster.stop();
            }
        });
    }
}
