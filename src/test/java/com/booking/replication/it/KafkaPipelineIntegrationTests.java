package com.booking.replication.it;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.testcontainers.containers.*;

import org.testcontainers.containers.output.Slf4jLogConsumer;

import org.testcontainers.images.RemoteDockerImage;


import lombok.NonNull;

/**
 * Created by bdevetak on 2/8/18.
 */
public class KafkaPipelineIntegrationTests {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPipelineIntegrationTests.class);

    private class Pipeline {

        public  Network network;

        public GenericContainer mysql;
        public GenericContainer zookeeper;
        public GenericContainer kafka;
        public GenericContainer replicator;

        public Pipeline() {

            network = Network.newNetwork();

            mysql = new GenericContainer("mysql:5.6.27")
                    .withNetwork(network)
                    .withNetworkAliases("mysql")
                    .withClasspathResourceMapping(
                            "my.cnf",
                            "/etc/mysql/conf.d/my.cnf",
                            BindMode.READ_ONLY)
                    .withClasspathResourceMapping(
                            "mysql_init_dbs.sh",
                            "/docker-entrypoint-initdb.d/mysql_init_dbs.sh",
                            BindMode.READ_ONLY)

            .withEnv("MYSQL_ROOT_PASSWORD", "mysqlPass");

            zookeeper = new GenericContainer("zookeeper:3.4")
                    .withNetwork(network)
                    .withNetworkAliases("zookeeper");

            kafka = new GenericContainer("wurstmeister/kafka:1.0.0")
                    .withNetwork(network)
                    .withNetworkAliases("kafka")
                    .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
                    .withEnv("KAFKA_CREATE_TOPICS", "replicator_test_kafka:1:1,replicator_validation:1:1");


//            String runReplicatorCommand = "java -jar /replicator/mysql-replicator.jar \\\n" +
//                    "    --parser bc \\\n" +
//                    "    --applier kafka \\\n" +
//                    "    --schema test \\\n" +
//                    "    --binlog-filename binlog.000001 \\\n" +
//                    "    --config-path /replicator/replicator-conf.yaml";

            replicator = new GenericContainer(new RemoteDockerImage("replicator-runner:latest"))
                    .withNetwork(network)
                    .withNetworkAliases("replicator");

            mysql.start();
            zookeeper.start();
            kafka.start();
            replicator.start();

        }
    }

    @Test
    public void testMySQL2KafkaPipeline() throws InterruptedException {
        Pipeline pipeline = new Pipeline();
        Thread.sleep(1000000);
    }

}
