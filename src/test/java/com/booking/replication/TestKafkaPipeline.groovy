package com.booking.replication

import groovy.json.JsonOutput
import groovy.json.JsonSlurper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Unroll

@GrabConfig(systemClassLoader = true)
import groovy.sql.Sql
import org.testcontainers.containers.output.OutputFrame
import org.testcontainers.containers.output.ToStringConsumer
import org.testcontainers.containers.output.WaitingConsumer
import org.testcontainers.shaded.io.netty.util.internal.shaded.org.jctools.queues.MessagePassingQueue

import java.util.concurrent.TimeUnit;

import static groovy.json.JsonOutput.*

import org.testcontainers.containers.*;
import org.testcontainers.images.RemoteDockerImage;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition

import static org.junit.Assert.assertTrue;

class TestKafkaPipeline {

    private static final Logger logger = LoggerFactory.getLogger(TestKafkaPipeline.class);

    public class Pipeline {

        public Network network;

        public GenericContainer mysql;
        public GenericContainer zookeeper;
        public GenericContainer kafka;
        public GenericContainer replicator;
        public GenericContainer graphite;

        private static final Integer KAFKA_PORT = 9092;
        private static final Integer ZOOKEEPER_PORT = 2181;

        public Pipeline() {

            network = Network.newNetwork();

            mysql = new GenericContainer("mysql:5.6.27")
                    .withNetwork(network)
                    .withNetworkAliases("mysql")
                    .withClasspathResourceMapping(
                    "my.cnf",
                    "/etc/mysql/conf.d/my.cnf",
                    BindMode.READ_ONLY
            )
                    .withClasspathResourceMapping(
                    "mysql_init_dbs.sh",
                    "/docker-entrypoint-initdb.d/mysql_init_dbs.sh",
                    BindMode.READ_ONLY
            )
                    .withEnv("MYSQL_ROOT_PASSWORD", "mysqlPass")
                    .withExposedPorts(3306)
            ;

            zookeeper = new GenericContainer("zookeeper:3.4")
                    .withNetwork(network)
                    .withNetworkAliases("zookeeper")
                    .withExposedPorts(ZOOKEEPER_PORT)
            ;

            kafka = new FixedHostPortGenericContainer("wurstmeister/kafka:1.0.0")
                    .withNetwork(network)
                    .withNetworkAliases("kafka")
                    .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
                    .withEnv("KAFKA_CREATE_TOPICS", "replicator_test_kafka:1:1,replicator_validation:1:1")
                    .withExposedPorts(9092)
            ;

            graphite = new GenericContainer("hopsoft/graphite-statsd:latest")
                    .withNetwork(network)
                    .withNetworkAliases("graphite")
                    .withExposedPorts(80)
            ;

            replicator = new GenericContainer(new RemoteDockerImage("replicator-runner:latest"))
                    .withNetwork(network)
                    .withNetworkAliases("replicator")
                    .withClasspathResourceMapping(
                    "replicator-conf.yaml",
                    "/replicator/replicator-conf.yaml",
                    BindMode.READ_ONLY
            )
            ;

        }

        public Thread startReplication() {

            def thread = Thread.start {
                def result = replicator.execInContainer(
                        "java",
                        "-jar", "/replicator/mysql-replicator.jar",
                        "--applier", "kafka",
                        "--schema", "test",
                        "--binlog-filename", "binlog.000001",
                        "--config-path", "/replicator/replicator-conf.yaml"
                );

                logger.info(result.stderr.toString());
                logger.info(result.stdout.toString());
            }

            return thread
        }

        public void start() {
            mysql.start();
            zookeeper.start();
            kafka.start();
            graphite.start();
            replicator.start();
        }

        public void shutdown() {
            replicator.stop();
            graphite.stop();
            kafka.stop();
            zookeeper.stop();
            mysql.stop();
        }

        public String getMySqlIP() {
            return mysql.getContainerIpAddress();
        }

        public Integer getMySqlPort() {
            return mysql.getMappedPort(3306);
        }

        public String getKafkaIP() {
            return kafka.getContainerIpAddress();
        }

        public Integer getKafkaPort() {
            return kafka.getMappedPort(KAFKA_PORT);
        }


        public String getGraphitelIP() {
            return graphite.getContainerIpAddress();
        }

        public Integer getGraphitePort() {
            return graphite.getMappedPort(80);
        }

        public static Properties getKafkaConsumerProperties(String broker) {
            // Consumer configuration
            Properties prop = new Properties();
            prop.put("bootstrap.servers", broker);
            prop.put("group.id", "testGroup");
            prop.put("auto.offset.reset", "latest");
            prop.put("enable.auto.commit", "false");
            prop.put("session.timeout.ms", "30000");
            prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            return prop;
        }

        public List<List<String>> readRowsFromKafka() {

            def result = kafka.execInContainer(
                    "/opt/kafka/bin/kafka-console-consumer.sh",
                    "--new-consumer",
                    "--bootstrap-server", "localhost:9092",
                    "--topic", "replicator_test_kafka",
                    "--timeout-ms", "5000",
                    "--from-beginning"
            )

            def messages = result.getStdout()

            def jsonSlurper = new JsonSlurper()

            def messageEntries = jsonSlurper.parseText(messages)

            def inserts =
                    messageEntries['rows'].findAll {
                        it["eventType"] == "INSERT"
                    }

            def rows = inserts.collect {
                [
                        it["eventColumns"]["pk_part_1"]["value"],
                        it["eventColumns"]["pk_part_2"]["value"],
                        it["eventColumns"]["randomint"]["value"],
                        it["eventColumns"]["randomvarchar"]["value"]
                ]
            }

            return rows;
        }

        public void InsertTestRowsToMySQL() {

            def urlReplicant = 'jdbc:mysql://' + this.getMySqlIP() + ":" + this.getMySqlPort() + '/test'
            def urlActiveSchema = 'jdbc:mysql://' + this.getMySqlIP() + ":" + this.getMySqlPort() + '/test_active_schema'

            logger.info("jdbc url: " + urlReplicant)

            def dbReplicant = [
                    url     : urlReplicant,
                    user    : 'root',
                    password: 'mysqlPass',
                    driver  : 'com.mysql.jdbc.Driver'
            ]

            def dbActiveSchema = [
                    url     : urlActiveSchema,
                    user    : 'root',
                    password: 'mysqlPass',
                    driver  : 'com.mysql.jdbc.Driver'
            ]

            def replicant = Sql.newInstance(
                    dbReplicant.url,
                    dbReplicant.user,
                    dbReplicant.password,
                    dbReplicant.driver
            )

            def activeSchema = Sql.newInstance(
                    dbActiveSchema.url,
                    dbActiveSchema.user,
                    dbActiveSchema.password,
                    dbActiveSchema.driver
            )

            replicant.connection.autoCommit = false
            activeSchema.connection.autoCommit = false

            // CREATE
            def sqlCreate = """
 CREATE TABLE IF NOT EXISTS
          sometable (
          pk_part_1         varchar(5) NOT NULL DEFAULT '',
          pk_part_2         int(11)    NOT NULL DEFAULT 0,
          randomInt         int(11)             DEFAULT NULL,
          randomVarchar     varchar(32)         DEFAULT NULL,
          PRIMARY KEY       (pk_part_1,pk_part_2),
          KEY randomVarchar (randomVarchar),
          KEY randomInt     (randomInt)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
"""

            replicant.execute("reset master")
            replicant.execute(sqlCreate);
            replicant.commit();

            activeSchema.execute("reset master")
            activeSchema.execute(sqlCreate);
            activeSchema.commit();

            // INSERT
            def testRows = [
                    ['tNMeE', '686140', '665726', 'PZBAAQSVoSxxFassEAQ'],
                    ['QrTSd', '1049668', '49070', 'cvjIXQiWLegvLs kXaKH'],
                    ['xzbTw', '4484536', '437616', 'pjFNkiZExAiHkKiJePMp'],
                    ['CIael', '2872792', '978231', 'RWURqZcnAGwQfRSisYcr'],
                    ['Cwd j', '2071578', '260864', 'jrotGtNYxRmpIKJbAEPd']
            ]

            testRows.each {
                row ->
                    try {
                        def sqlString = """
                            INSERT INTO
                                sometable (
                                    pk_part_1,       
                                    pk_part_2,
                                    randomInt,
                                    randomVarchar
                                )
                                values (
                                    ${row[0]},
                                    ${row[1]},
                                    ${row[2]},
                                    ${row[3]}
                                )
                        """
                        logger.info(sqlString)
                        replicant.execute(sqlString)
                        replicant.commit()
                    } catch (Exception ex) {
                        replicant.rollback()
                    }
            }

            // SELECT
            def resultSet = []
            replicant.eachRow('select * from sometable') {
                row ->
                    resultSet.add([
                            pk_part_1    : row.pk_part_1,
                            pk_part_2    : row.pk_part_2,
                            randomInt    : row.randomInt,
                            randomVarchar: row.randomVarchar
                    ])
            }

            logger.info("retrieved from MySQL: " + prettyPrint(toJson(resultSet)))

            replicant.close()
            activeSchema.close()
        }
    }
}

class PipelineTestsSpec extends Specification {

    @Shared  pipeline = new TestKafkaPipeline.Pipeline();

    @Unroll
    def "pipelineTransmitRows{#result == #expected}"()  {

        TestKafkaPipeline.Pipeline
        pipeline.start();

        Thread.sleep(5000);

        // ====================================================================
        // Insert test rows to MySQL
        pipeline.InsertTestRowsToMySQL()

        // ====================================================================
        // Start replication
        def replicatorCmdHandle = pipeline.startReplication()
        Thread.sleep(1000);

        // ====================================================================
        // Read from Kafka, shutdown and compare

        def realResults = pipeline.readRowsFromKafka()

        pipeline.shutdown()
        replicatorCmdHandle.join();

        expect:
        result == expected

        where:
        result << realResults.collect{
            it[0] + "|" +
            it[1] + "|" +
            it[2] + "|" +
            it[3]
        }
        expected << [
                "tNMeE|686140|665726|PZBAAQSVoSxxFassEAQ",
                "QrTSd|1049668|49070|cvjIXQiWLegvLs kXaKH",
                "xzbTw|4484536|437616|pjFNkiZExAiHkKiJePMp",
                "CIael|2872792|978231|RWURqZcnAGwQfRSisYcr"
        ]
    }
}