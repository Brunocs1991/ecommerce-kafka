package br.com.brunocs;

import br.com.brunocs.database.LocalDatabase;
import br.com.brunocs.kafka.consumer.ConsumerService;
import br.com.brunocs.kafka.consumer.ServiceRunner;
import br.com.brunocs.kafka.dispatcher.KafkaDispatch;
import br.com.brunocs.kafka.utils.Message;
import br.com.brunocs.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public class FraudeDetectorService implements ConsumerService<Order> {

    private final LocalDatabase database;

    public FraudeDetectorService() throws SQLException {
        this.database = new LocalDatabase("fraud_database");
        this.database.createIfNotExists("CREATE TABLE Orders (" +
                "uuid VARCHAR(200) PRIMARY KEY, " +
                "is_fraud boolean)");
    }

    private static final Logger logger = LoggerFactory.getLogger(FraudeDetectorService.class);
    private final KafkaDispatch<Order> orderDispatecher = new KafkaDispatch();

    public static void main(String[] args) {
        new ServiceRunner<>(FraudeDetectorService::new).start(1);
    }

    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException, SQLException {
        var message = record.value();
        var order = message.getPayload();
        System.out.println("---------------------");
        System.out.println("Processing new order, checking for fraud");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        if (wasProcessed(order)) {
            System.out.println("Order was already processed uuid " + order.getOrderId());
            return;
        }
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            // ignoring
            logger.error(e.getMessage(), e);
        }


        if (isFraud(order)) {
            database.update("INSERT INTO Orders (uuid, is_fraud) VALUES (?, true) ", order.getOrderId());
            System.out.println("Order is not valid - fraud detected");
            orderDispatecher.send(
                    "ECOMMERCE_ORDER_REJECTED",
                    order.getEmail(),
                    record.value().getId().continueWith(FraudeDetectorService.class.getSimpleName()),
                    order
            );
        } else {
            database.update("INSERT INTO Orders (uuid, is_fraud) VALUES (?, false) ", order.getOrderId());
            System.out.println("Order is valid - no fraud detected");
            orderDispatecher.send(
                    "ECOMMERCE_ORDER_APPROVED",
                    order.getEmail(),
                    record.value().getId().continueWith(FraudeDetectorService.class.getSimpleName()),
                    order
            );
        }
    }

    private boolean wasProcessed(Order order) throws SQLException {
        return database.query("SELECT uuid FROM Orders WHERE uuid = ? limit 1", order.getOrderId()).next();
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
