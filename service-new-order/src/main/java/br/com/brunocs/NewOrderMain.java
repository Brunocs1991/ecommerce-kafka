package br.com.brunocs;

import br.com.brunocs.kafka.KafkaDispatch;
import br.com.brunocs.model.Email;
import br.com.brunocs.model.Order;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var orderDispatcher = new KafkaDispatch<Order>()) {
            try (var emailDispatcher = new KafkaDispatch<Email>()) {
                for (int i = 0; i < 1000; i++) {
                    var email = Math.random() + "@email.com";
                    for (int j = 0; j < 10; j++) {
                        var orderId = UUID.randomUUID().toString();
                        var amount = BigDecimal.valueOf(Math.random() * 5000 + 1);

                        var order = new Order(orderId, amount, email);
                        orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

                        var texto = "Thank you for your order! We are processing your order";
                        var emailCode = new Email("new order", texto);
                        emailDispatcher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);
                    }
                }
            }
        }
    }
}
