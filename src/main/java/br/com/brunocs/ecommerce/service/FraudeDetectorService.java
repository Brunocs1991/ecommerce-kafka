package br.com.brunocs.ecommerce.service;

import br.com.brunocs.ecommerce.kafka.KafkaService;
import br.com.brunocs.ecommerce.models.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class FraudeDetectorService {

    private static final Logger logger = LoggerFactory.getLogger(FraudeDetectorService.class);

    public static void main(String[] args) {
        var fraudeDetectorService = new FraudeDetectorService();
        try (var service = new KafkaService(
                FraudeDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudeDetectorService::parse,
                Order.class,
                Map.of()
        )) {

            service.run();
        }
    }

    public void parse(ConsumerRecord<String, Order> record) {
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
