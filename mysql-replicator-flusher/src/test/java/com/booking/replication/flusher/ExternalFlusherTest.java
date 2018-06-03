package com.booking.replication.flusher;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class ExternalFlusherTest {

    @Test
    public void flush() throws IOException {
        HashMap<String, String> configuration = new HashMap<>();
        configuration.put(ExternalFlusher.Configuration.MYSQL_HOSTNAME, "hostname");
        configuration.put(ExternalFlusher.Configuration.MYSQL_SCHEMA, "schema");
        configuration.put(ExternalFlusher.Configuration.MYSQL_USERNAME, "user");
        configuration.put(ExternalFlusher.Configuration.MYSQL_PASSWORD, "password");
        ExternalFlusher flusher = new ExternalFlusher(configuration);
        flusher.flush(Arrays.asList("a"));
    }
}