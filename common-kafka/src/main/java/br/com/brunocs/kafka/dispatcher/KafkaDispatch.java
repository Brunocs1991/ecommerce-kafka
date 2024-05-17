package br.com.brunocs.kafka.dispatcher;

import br.com.brunocs.kafka.utils.CorrelationId;
import br.com.brunocs.kafka.utils.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatch<T> implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaDispatch.class);

    private final KafkaProducer<String, Message<T>> producer;

    public KafkaDispatch() {
        this.producer = new KafkaProducer<>(properties());
    }


    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    public void sendAsync(String topico, String key, CorrelationId correlationId, T payload) {
        getRecordMetadataFuture(topico, key, correlationId, payload);
    }

    public void send(String topico, String key, CorrelationId correlationId, T payload) throws ExecutionException, InterruptedException {
        var future = getRecordMetadataFuture(topico, key, correlationId, payload);
        future.get();
    }

    private Future<RecordMetadata> getRecordMetadataFuture(String topico, String key, CorrelationId correlationId, T payload) {
        var value = new Message<>(correlationId.continueWith(String.format("_%s", topico)), payload);
        var record = new ProducerRecord<>(topico, key, value);
        return producer.send(record, getCallback());
    }

    private static Callback getCallback() {
        return (data, ex) -> {
            if (ex != null) {
                logger.error(ex.getMessage(), ex);
            }
            System.out.println(data.topic() + "::: " + data.partition() + "::: " + data.offset() + "/timestamp" + data.timestamp());
        };
    }

    @Override
    public void close() {
        this.producer.close();
    }
}
