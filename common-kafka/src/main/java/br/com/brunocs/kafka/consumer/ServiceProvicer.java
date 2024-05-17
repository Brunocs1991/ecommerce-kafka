package br.com.brunocs.kafka.consumer;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class ServiceProvicer<T> implements Callable<Void> {
    private final ServiceFactory<T> factory;

    public ServiceProvicer(ServiceFactory<T> factory) {
        this.factory = factory;
    }

    public Void call() throws ExecutionException, InterruptedException {

        var myService = factory.create();
        try (var service = new KafkaService(
                myService.getConsumerGroup(),
                myService.getTopic(),
                myService::parse,
                Map.of())) {
            service.run();
        }
        return null;
    }
}
