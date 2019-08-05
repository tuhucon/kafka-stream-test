package com.example.kafkastreamtest;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.To;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication

public class KafkaStreamTestApplication {

    public static Topology getTopology() {
        Topology topology = new Topology();

        KeyValueBytesStoreSupplier keyValueBytesStoreSupplier = Stores.inMemoryKeyValueStore("store");
        StoreBuilder<KeyValueStore<String, String>> storeBuilder = Stores.keyValueStoreBuilder(keyValueBytesStoreSupplier, Serdes.String(), Serdes.String());

        topology
                .addSource("source", Serdes.String().deserializer(), Serdes.String().deserializer(), "source-test")
                .addProcessor("upper", () -> new StringUpperProcessor("store"), "source")
                .addSink("sink", "sink-test", Serdes.String().serializer(), Serdes.String().serializer(), "upper");

        topology.addStateStore(storeBuilder, "upper");
        return topology;
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaStreamTestApplication.class, args);
    }

}
