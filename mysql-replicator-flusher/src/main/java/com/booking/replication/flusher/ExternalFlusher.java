package com.booking.replication.flusher;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class ExternalFlusher implements Flusher {
    private static final Logger LOG = Logger.getLogger(ExternalFlusher.class.getName());

    public interface Configuration {
        String MYSQL_HOSTNAME = "mysql.hostname";
        String MYSQL_PORT = "mysql.port";
        String MYSQL_SCHEMA = "mysql.schema";
        String MYSQL_USERNAME = "mysql.username";
        String MYSQL_PASSWORD = "mysql.password";
    }

    private Function<Collection<String>, ProcessBuilder> partialProcessBuilder;

    public ExternalFlusher(Map<String, String> configuration) {
        String hostname = configuration.get(Configuration.MYSQL_HOSTNAME);
        String port = configuration.getOrDefault(Configuration.MYSQL_PORT, "3306");
        String schema = configuration.get(Configuration.MYSQL_SCHEMA);
        String username = configuration.get(Configuration.MYSQL_USERNAME);
        String password = configuration.get(Configuration.MYSQL_PASSWORD);

        Objects.requireNonNull(hostname, String.format("Configuration required: %s", Configuration.MYSQL_HOSTNAME));
        Objects.requireNonNull(schema, String.format("Configuration required: %s", Configuration.MYSQL_SCHEMA));
        Objects.requireNonNull(username, String.format("Configuration required: %s", Configuration.MYSQL_USERNAME));
        Objects.requireNonNull(password, String.format("Configuration required: %s", Configuration.MYSQL_PASSWORD));

        this.partialProcessBuilder = (Collection<String> tables) -> {
            ProcessBuilder pb = null;
            try {
                pb = this.getProcessBuilder(hostname, Integer.parseInt(port), schema, username, password, tables);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                return pb;
            }
        };
    }

    private String extractResourcesFile(String path) throws IOException {
        Path tempFile = Files.createTempFile("flusher.py", null);
        tempFile.toFile().deleteOnExit();
        Files.copy(this.getClass().getResourceAsStream(path), tempFile, StandardCopyOption.REPLACE_EXISTING);
        return tempFile.toAbsolutePath().toString();
    }

    private String createMyCnf(String username, String password) throws IOException {
        Path tempFile = Files.createTempFile(".my.cnf", null);
        tempFile.toFile().deleteOnExit();
        Files.write(tempFile, String.format("[client]\nusername=%s\npassword=%s\n", username, password).getBytes());
        return tempFile.toAbsolutePath().toString();
    }

    private ProcessBuilder getProcessBuilder(String hostname, int i, String schema, String username, String password, Collection<String> tables) throws IOException {
        String path = "/binlog-flusher/data-flusher.py";
        String script = extractResourcesFile(path);
        String mycnf = createMyCnf(username, password);
        String table = tables.stream().collect(Collectors.joining(","));

        List<String> command = Arrays.asList(
                "python",
                script,
                "--host", hostname,
                "--db", schema,
                "--mycnf", mycnf,
                "--table", table,
                "--stop-slave",
                "--start-slave"
        );

        return new ProcessBuilder(command);
    }

    @Override
    public void flush(Collection<String> tables) throws IOException {
        this.partialProcessBuilder.apply(tables).start();
    }
}
