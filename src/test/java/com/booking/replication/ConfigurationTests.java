package com.booking.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertTrue;

/**
 * Created by bdevetak on 2/7/18.
 */
public class ConfigurationTests {

    @Test
    public void testConvertNullToStringOnConfig() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        String config = "replication_schema:\n" +
                "    name:      'test'\n" +
                "    username:  '__USER__'\n" +
                "    password:  '__PASS__'\n" +
                "    host_pool: ['localhost2', 'localhost']\n" +
                "\n" +
                "metadata_store:\n" +
                "    username: '__USER__'\n" +
                "    password: '__PASS__'\n" +
                "    host:     'localhost'\n" +
                "    database: 'test_active_schema'\n" +
                "    file:\n" +
                "        path: '/opt/replicator/replicator_metadata'\n" +
                "kafka:\n" +
                "    broker: \"kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092\"\n" +
                "    topic:  test\n" +
                "    tables: [\"sometable\"]\n" +
                "\n" +
                "    partitioning_method: 2\n" +
                "    partition_columns:\n" +
                "        TestTable: TestColumn\n" +
                "augmenter:\n" +
                "    convertNullToString: 1\n" +
                "mysql_failover:\n" +
                "    pgtid:\n" +
                "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        boolean converNullToString = configuration.getAugmenterConverNullToString();
        assertTrue(converNullToString == true);
    }

    @Test
    public void testConvertNullToStringOffConfig() throws Exception {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        String config = "replication_schema:\n" +
                "    name:      'test'\n" +
                "    username:  '__USER__'\n" +
                "    password:  '__PASS__'\n" +
                "    host_pool: ['localhost2', 'localhost']\n" +
                "\n" +
                "metadata_store:\n" +
                "    username: '__USER__'\n" +
                "    password: '__PASS__'\n" +
                "    host:     'localhost'\n" +
                "    database: 'test_active_schema'\n" +
                "    file:\n" +
                "        path: '/opt/replicator/replicator_metadata'\n" +
                "kafka:\n" +
                "    broker: \"kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092\"\n" +
                "    topic:  test\n" +
                "    tables: [\"sometable\"]\n" +
                "\n" +
                "    partitioning_method: 2\n" +
                "    partition_columns:\n" +
                "        TestTable: TestColumn\n" +
                "augmenter:\n" +
                "    convertNullToString: 0\n" +
                "mysql_failover:\n" +
                "    pgtid:\n" +
                "        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'\n" +
                "        p_gtid_prefix: \"use `pgtid_meta`;\"\n";

        InputStream in = new ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name()));
        Configuration configuration = mapper.readValue(in, Configuration.class);

        boolean converNullToString = configuration.getAugmenterConverNullToString();
        assertTrue(converNullToString == false);
    }
}
