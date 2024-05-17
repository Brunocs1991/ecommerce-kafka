package br.com.brunocs.interfaces;

import br.com.brunocs.kafka.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public interface ConsumerFunction<T> {
    void consumer(ConsumerRecord<String, Message<T>> record) throws Exception;
}
