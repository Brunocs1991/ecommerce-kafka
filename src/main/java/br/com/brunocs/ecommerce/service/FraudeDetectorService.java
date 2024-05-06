package br.com.brunocs.ecommerce.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FraudeDetectorService {

    private static final Logger logger = LoggerFactory.getLogger(FraudeDetectorService.class);

    public static void main(String[] args) {
        var fraudeDetectorService = new FraudeDetectorService();
        var service = new KafkaService(
                FraudeDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudeDetectorService::parse);
        service.run();
    }

    public void parse(ConsumerRecord<String, String> record) {
        System.out.println("---------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // ignoring
            logger.error(e.getMessage(), e);
        }
        System.out.println("Order processed");
    }

}
