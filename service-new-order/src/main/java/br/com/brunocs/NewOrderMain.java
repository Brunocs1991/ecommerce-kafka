package br.com.brunocs;

import br.com.brunocs.kafka.dispatcher.KafkaDispatch;
import br.com.brunocs.kafka.utils.CorrelationId;
import br.com.brunocs.model.Order;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatch<Order>()) {
            for (int i = 0; i < 100; i++) {
                var email = Math.random() + "@email.com";
                for (int j = 0; j < 10; j++) {
                    var orderId = UUID.randomUUID().toString();
                    var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);

                    var order = new Order(orderId, amount, email);
                    var id = new CorrelationId(NewOrderMain.class.getSimpleName());
                    orderDispatcher.send(
                            "ECOMMERCE_NEW_ORDER",
                            email,
                            id,
                            order
                    );
                }
            }

        }
    }
}
