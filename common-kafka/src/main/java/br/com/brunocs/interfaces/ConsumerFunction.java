package br.com.brunocs.interfaces;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public interface ConsumerFunction<T> {
    void consumer(ConsumerRecord<String, T> record) throws Exception;
}
