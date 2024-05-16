package br.com.brunocs;

import br.com.brunocs.kafka.KafkaService;
import br.com.brunocs.model.Email;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EmailService {

    private static final Logger logger = LoggerFactory.getLogger(EmailService.class);

    public static void main(String[] args) {
        var emailService = new EmailService();
        try (var service = new KafkaService(
                EmailService.class.getSimpleName(),
                "ECOMMERCE_SEND_EMAIL",
                emailService::parse,
                Email.class,
                Map.of()
        )) {

            service.run();
        }
    }

    private void parse(ConsumerRecord<String, Email> record) {
        System.out.println("---------------------");
        System.out.println("sending email");
        System.out.println(record.key());
        System.out.println(record.value());
        System.out.println(record.partition());
        System.out.println(record.offset());
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        System.out.println("Email Send");


    }
}