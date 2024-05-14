package br.com.brunocs.interfaces;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction<T> {
    void consumer(ConsumerRecord<String, T> record);
}
