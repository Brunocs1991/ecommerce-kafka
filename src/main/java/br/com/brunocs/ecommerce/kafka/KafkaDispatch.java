package br.com.brunocs.ecommerce.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatch implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(KafkaDispatch.class);

    private final KafkaProducer<String, String> producer;

    public KafkaDispatch() {
        this.producer = new KafkaProducer<>(properties());
    }


    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    public void send( String topico, String key, String value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topico, key, value);
        producer.send(record, getCallback()).get();
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
