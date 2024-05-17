package br.com.brunocs;

import br.com.brunocs.kafka.KafkaDispatch;
import br.com.brunocs.kafka.KafkaService;
import br.com.brunocs.kafka.Message;
import br.com.brunocs.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class FraudeDetectorService {

    private static final Logger logger = LoggerFactory.getLogger(FraudeDetectorService.class);
    private final KafkaDispatch<Order> orderDispatecher = new KafkaDispatch();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var fraudeDetectorService = new FraudeDetectorService();
        try (var service = new KafkaService(
                FraudeDetectorService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                fraudeDetectorService::parse,
                Map.of()
        )) {

            service.run();
        }
    }


    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
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
        var order = record.value().getPayload();
        if(isFraud(order)) {
            System.out.println("Order is not valid - fraud detected");
            orderDispatecher.send(
                    "ECOMMERCE_ORDER_REJECTED",
                    order.getEmail(),
                    record.value().getId().continueWith(FraudeDetectorService.class.getSimpleName()),
                    order
            );
        }else{
            System.out.println("Order is valid - no fraud detected");
            orderDispatecher.send(
                    "ECOMMERCE_ORDER_APPROVED",
                    order.getEmail(),
                    record.value().getId().continueWith(FraudeDetectorService.class.getSimpleName()),
                    order
            );
        }
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}
