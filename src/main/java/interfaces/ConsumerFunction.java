package interfaces;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface ConsumerFunction {
    void consumer(ConsumerRecord<String, String> record);
}
