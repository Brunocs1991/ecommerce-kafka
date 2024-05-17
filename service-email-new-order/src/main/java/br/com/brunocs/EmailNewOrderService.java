package br.com.brunocs;


import br.com.brunocs.kafka.consumer.KafkaService;
import br.com.brunocs.kafka.dispatcher.KafkaDispatch;
import br.com.brunocs.kafka.utils.Message;
import br.com.brunocs.model.Email;
import br.com.brunocs.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class EmailNewOrderService {

    private final KafkaDispatch<Email> emailDispatcher = new KafkaDispatch();

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var emailService = new EmailNewOrderService();
        try (var service = new KafkaService<>(
                EmailNewOrderService.class.getSimpleName(),
                "ECOMMERCE_NEW_ORDER",
                emailService::parse,
                Map.of()
        )) {
            service.run();
        }
    }


    private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
        var order = record.value().getPayload();
        System.out.println("______________________________________");
        System.out.println("Processing new order, preparing email");
        System.out.println(order);

        var texto = "Thank you for your order! We are processing your order";
        var emailCode = new Email("new order", texto);
        emailDispatcher.send(
                "ECOMMERCE_SEND_EMAIL",
                order.getEmail(),
                record.value().getId().continueWith(EmailNewOrderService.class.getSimpleName()),
                emailCode
        );
    }
}
