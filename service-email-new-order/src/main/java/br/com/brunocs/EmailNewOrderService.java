package br.com.brunocs;


import br.com.brunocs.kafka.consumer.ConsumerService;
import br.com.brunocs.kafka.consumer.ServiceRunner;
import br.com.brunocs.kafka.dispatcher.KafkaDispatch;
import br.com.brunocs.kafka.utils.Message;
import br.com.brunocs.model.Email;
import br.com.brunocs.model.Order;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.ExecutionException;

public class EmailNewOrderService implements ConsumerService<Order> {

    private final KafkaDispatch<Email> emailDispatcher = new KafkaDispatch();

    public static void main(String[] args) {
        new ServiceRunner<>(EmailNewOrderService::new).start(1);
    }


    @Override
    public void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
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

    @Override
    public String getTopic() {
        return "ECOMMERCE_NEW_ORDER";
    }

    @Override
    public String getConsumerGroup() {
        return EmailNewOrderService.class.getSimpleName();
    }
}
