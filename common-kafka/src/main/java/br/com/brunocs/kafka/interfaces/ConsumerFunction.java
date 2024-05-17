package br.com.brunocs.kafka.interfaces;

import br.com.brunocs.kafka.utils.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction<T> {
    void consumer(ConsumerRecord<String, Message<T>> record) throws Exception;
}
