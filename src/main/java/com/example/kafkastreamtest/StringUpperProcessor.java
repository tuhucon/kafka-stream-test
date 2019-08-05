package com.example.kafkastreamtest;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class StringUpperProcessor implements Processor<String, String> {
    ProcessorContext context;
    String storeName;
    KeyValueStore<String, String> store;

    public StringUpperProcessor(String storeName) {
        this.storeName = storeName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.store = (KeyValueStore<String, String>)context.getStateStore(storeName);
    }

    @Override
    public void process(String key, String value) {
        if (key != null) {
            String oldValue = store.get(key);
            if (oldValue == null) {
                store.put(key, value);
            } else if (oldValue.compareTo(value) < 0) {
                store.put(key, value);
            }
        }
        context.forward(key, value.toUpperCase());
    }

    @Override
    public void close() {

    }
}
