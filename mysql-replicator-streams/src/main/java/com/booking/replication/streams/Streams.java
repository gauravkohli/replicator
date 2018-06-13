package com.booking.replication.streams;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@SuppressWarnings("unused")
public interface Streams<Input, Output> {
    class Task {
        private final int current;
        private final int total;

        Task(int current, int total) {
            this.current = current;
            this.total = total;
        }

        public int getCurrent() {
            return this.current;
        }

        public int getTotal() {
            return this.total;
        }
    }

    Streams<Input, Output> start() throws InterruptedException;

    Streams<Input, Output> wait(long timeout, TimeUnit unit) throws InterruptedException;

    void join() throws InterruptedException;

    void stop() throws InterruptedException;

    void onException(Consumer<Exception> handler);

    boolean push(Input input);

    int size();

    static <Input> StreamsBuilderFrom<Input, Input> builder() {
        return new StreamsBuilder<>();
    }
}
