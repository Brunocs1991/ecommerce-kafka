package br.com.brunocs.kafka.consumer;

import java.util.concurrent.Executors;

public class ServiceRunner<T> {


    private final ServiceProvicer<T> provider;

    public ServiceRunner(ServiceFactory<T> factory) {
        this.provider = new ServiceProvicer<>(factory);
    }

    public void start(int threadCount) {
        var poll = Executors.newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            poll.submit(provider);
        }

    }
}
