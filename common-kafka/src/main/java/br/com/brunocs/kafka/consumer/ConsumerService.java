package br.com.brunocs.kafka.consumer;

import br.com.brunocs.kafka.utils.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public interface ConsumerService<T> {

    void parse(ConsumerRecord<String, Message<T>> record) throws Exception;

    String getTopic();

    String getConsumerGroup();
}
