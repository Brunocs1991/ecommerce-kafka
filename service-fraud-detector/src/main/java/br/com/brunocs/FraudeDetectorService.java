package br.com.brunocs;

import br.com.brunocs.kafka.consumer.ConsumerService;
import br.com.brunocs.kafka.consumer.ServiceRunner;
import br.com.brunocs.kafka.dispatcher.KafkaDispatch;
import br.com.brunocs.kafka.utils.Message;
import br.com.brunocs.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.concurrent.ExecutionException;

public class FraudeDetectorService implements ConsumerService<Order> {

    private static final Logger logger = LoggerFactory.getLogger(FraudeDetectorService.class);
    private final KafkaDispatch<Order> orderDispatecher = new KafkaDispatch();

    public static void main(String[] args) {
        new ServiceRunner<>(FraudeDetectorService::new).start(5);
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
        if (isFraud(order)) {
            System.out.println("Order is not valid - fraud detected");
            orderDispatecher.send(
                    "ECOMMERCE_ORDER_REJECTED",
                    order.getEmail(),
                    record.value().getId().continueWith(FraudeDetectorService.class.getSimpleName()),
                    order
            );
        } else {
            System.out.println("Order is valid - no fraud detected");
            orderDispatecher.send(
                    "ECOMMERCE_ORDER_APPROVED",
                    order.getEmail(),
                    record.value().getId().continueWith(FraudeDetectorService.class.getSimpleName()),
                    order
            );
        }
    }

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return FraudeDetectorService.class.getSimpleName();
    }

    private boolean isFraud(Order order) {
        return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
    }

}
