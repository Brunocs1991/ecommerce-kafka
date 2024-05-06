package br.com.brunocs.ecommerce;

import br.com.brunocs.ecommerce.kafka.KafkaDispatch;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var dispatch = new KafkaDispatch()) {
            for (int i = 0; i < 2000; i++) {
                var key = UUID.randomUUID().toString();
                var value = key + ",12346,7894589754";
                dispatch.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Thank you for your order! We are processing your order";
                dispatch.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }
}
