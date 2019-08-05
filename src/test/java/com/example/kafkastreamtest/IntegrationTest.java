package com.example.kafkastreamtest;

import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.junit.ClassRule;

public class IntegrationTest {
    @ClassRule
    EmbeddedKafkaCluster embeddedKafkaCluster;

}
