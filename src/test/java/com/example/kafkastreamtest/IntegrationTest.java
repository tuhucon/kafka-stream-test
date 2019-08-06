package com.example.kafkastreamtest;

import kafka.server.KafkaConfig$;
import kafka.tools.ConsoleProducer;
import net.mguenther.kafka.junit.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.processor.To;
import org.hamcrest.Matchers;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

//@RunWith(SpringRunner.class)
//@SpringBootTest
public class IntegrationTest extends IntegrationBase {
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