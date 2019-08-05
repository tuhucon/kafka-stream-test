package com.example.kafkastreamtest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.swing.plaf.PanelUI;
import java.util.Properties;

import static org.junit.Assert.*;

public class TopologyTest {

    TopologyTestDriver topologyTestDriver;
    ConsumerRecordFactory<String, String> recordFactory = new ConsumerRecordFactory<>(Serdes.String().serializer(), Serdes.String().serializer());
    KeyValueStore<String, String> store;

    @Before
    public void setup() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        Topology topology = KafkaStreamTestApplication.getTopology();

        topologyTestDriver = new TopologyTestDriver(topology, properties);
        store = topologyTestDriver.getKeyValueStore("store");
    }

    @Test
    public void testNormalCase() {
        topologyTestDriver.pipeInput(recordFactory.create("source-test", "tuhucon", "tuhucon"));
        ProducerRecord<String, String> record = getRecordFromSinkTopic();
        OutputVerifier.compareKeyValue(record, "tuhucon", "TUHUCON");
    }

    @Test
    public void testKeyNull() {
        topologyTestDriver.pipeInput(recordFactory.create("source-test", null, "tuhucon"));
        ProducerRecord<String, String> record = getRecordFromSinkTopic();
        OutputVerifier.compareKeyValue(record, null, "TUHUCON");
    }

    @Test
    public void testValueEmpty() {
        topologyTestDriver.pipeInput(recordFactory.create("source-test", "tuhucon", ""));
        ProducerRecord<String, String> record = getRecordFromSinkTopic();
        OutputVerifier.compareKeyValue(record, "tuhucon", "");
    }

    @Test
    public void testKeyNotInState() {
        assertNull("init state of key must be null", store.get("A"));
        topologyTestDriver.pipeInput(recordFactory.create("source-test", "A", "A"));
        assertEquals("A", store.get("A"));
        topologyTestDriver.pipeInput(recordFactory.create("source-test", "A", "B"));
        assertEquals("B", store.get("A"));
    }

    @Test
    public void testKeyInState() {
        store.put("A", "D");
        assertNotNull(store.get("A"));
        topologyTestDriver.pipeInput(recordFactory.create("source-test", "A", "x"));
        assertEquals("x", store.get("A"));
    }

    private ProducerRecord<String, String> getRecordFromSinkTopic() {
       return topologyTestDriver.readOutput("sink-test", Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @After
    public void after() {
        topologyTestDriver.close();
    }
}
