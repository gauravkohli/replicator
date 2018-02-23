package com.booking.replication

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.nio.charset.StandardCharsets

class ConfigLoader {

    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    def configTests = [

"""
replication_schema:
    name:      'test'
    username:  '__USER__'
    password:  '__PASS__'
    host_pool: ['localhost2', 'localhost']
    
metadata_store:
    username: '__USER__'
    password: '__PASS__'
    host:     'localhost'
    database: 'test_active_schema'
    file:
        path: '/opt/replicator/replicator_metadata'

kafka:
    broker: \"kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092\"
    topic:  test
    tables: [\"sometable\"]
    partitioning_method: 2
    partition_columns:
        TestTable: TestColumn

mysql_failover:
    pgtid:
        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'
        p_gtid_prefix: "use `pgtid_meta`;"
        
augmenter:
    convert_null_to_string: 1
"""
,

"""
replication_schema:
    name:      'test'
    username:  '__USER__'
    password:  '__PASS__'
    host_pool: ['localhost2', 'localhost']
    
metadata_store:
    username: '__USER__'
    password: '__PASS__'
    host:     'localhost'
    database: 'test_active_schema'
    file:
        path: '/opt/replicator/replicator_metadata'

kafka:
    broker: \"kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092\"
    topic:  test
    tables: [\"sometable\"]
    partitioning_method: 2
    partition_columns:
        TestTable: TestColumn

mysql_failover:
    pgtid:
        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'
        p_gtid_prefix: "use `pgtid_meta`;"
        
augmenter:
    convert_null_to_string: 0
"""

,


"""
replication_schema:
    name:      'test'
    username:  '__USER__'
    password:  '__PASS__'
    host_pool: ['localhost2', 'localhost']
    
metadata_store:
    username: '__USER__'
    password: '__PASS__'
    host:     'localhost'
    database: 'test_active_schema'
    file:
        path: '/opt/replicator/replicator_metadata'

kafka:
    broker: \"kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092\"
    topic:  test
    tables: [\"sometable\"]
    partitioning_method: 1
    partition_columns:
        TestTable: TestColumn

mysql_failover:
    pgtid:
        p_gtid_pattern: '(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})'
        p_gtid_prefix: "use `pgtid_meta`;"
        
augmenter:
    convert_null_to_string: 0
"""

    ]

    def getTests() {
        def tests = configTests.collect{
            config ->
            mapper.readValue(
                    new java.io.ByteArrayInputStream(config.getBytes(StandardCharsets.UTF_8.name())),
                    Configuration.class
            )
        }
        return tests
    }
}

class ConfigurationTestsSpec extends Specification {

    @Shared cfg = new ConfigLoader();

    @Unroll
    def "configTests{#result == #expected}"() {

        expect:
        result == expected

        where:
        result << cfg.getTests().collect{
            it.getActiveSchemaDB() + "|" +
            it.augmenterConverNullToString + "|" +
            it.kafkaPartitioningMethod
        }
        expected << [
            "test_active_schema|true|2",
            "test_active_schema|false|2",
            "test_active_schema|false|1"
        ]
    }
}
