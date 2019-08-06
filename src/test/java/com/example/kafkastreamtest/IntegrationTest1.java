package com.example.kafkastreamtest;

import kafka.server.KafkaConfig$;
import net.mguenther.kafka.junit.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.List;
import java.util.Properties;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

//@RunWith(SpringRunner.class)
//@SpringBootTest
public class IntegrationTest1 extends IntegrationBase{
    @BeforeClass
    public static void init() {
        embeddedKafkaCluster.createTopic(TopicConfig.forTopic("source-test").build());
        embeddedKafkaCluster.createTopic(TopicConfig.forTopic("sink-test").build());
    }

    @AfterClass
    public static void clear() {
        embeddedKafkaCluster.deleteTopic("source-test");
        embeddedKafkaCluster.deleteTopic("sink-test");
    }

    @Test
    public void produceMessage() throws InterruptedException{
        Properties p = new Properties();
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());

        SendValues<String> sendValues = SendValues.to("source-test", "Tuhucon").withAll(p).build();
        embeddedKafkaCluster.send(sendValues);

        ReadKeyValues<String, String> readKeyValues = ReadKeyValues.from("source-test").withAll(p).build();
        List<String> values = embeddedKafkaCluster.readValues(readKeyValues);

        assertThat(values.size(), equalTo(1));

    }
}