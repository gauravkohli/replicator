package com.booking.replication.it;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@GrabConfig(systemClassLoader = true)
import groovy.sql.Sql;
import static groovy.json.JsonOutput.*

import org.testcontainers.containers.*;

import org.testcontainers.images.RemoteDockerImage;

/**
 * Created by bdevetak on 2/8/18.
 */
class TestKafkaPipeline {

    private static final Logger logger = LoggerFactory.getLogger(TestKafkaPipeline.class);

    private class Pipeline {

        public  Network network;

        public GenericContainer mysql;
        public GenericContainer zookeeper;
        public GenericContainer kafka;
        public GenericContainer replicator;
        public GenericContainer graphite;

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
                    .withEnv("MYSQL_ROOT_PASSWORD", "mysqlPass")
                    .withExposedPorts(3306)
            ;

            zookeeper = new GenericContainer("zookeeper:3.4")
                    .withNetwork(network)
                    .withNetworkAliases("zookeeper")
            ;

            kafka = new GenericContainer("wurstmeister/kafka:1.0.0")
                    .withNetwork(network)
                    .withNetworkAliases("kafka")
                    .withEnv("KAFKA_ZOOKEEPER_CONNECT", "zookeeper:2181")
                    .withEnv("KAFKA_CREATE_TOPICS", "replicator_test_kafka:1:1,replicator_validation:1:1")
            ;

            graphite = new GenericContainer("hopsoft/graphite-statsd:latest")
                    .withNetwork(network)
                    .withNetworkAliases("graphite")
                    .withExposedPorts(80)
            ;


            def startReplication = "java -jar /replicator/mysql-replicator.jar --parser bc --applier kafka --schema test --binlog-filename binlog.000001 --config-path /replicator/replicator-conf.yaml"

            replicator = new GenericContainer(new RemoteDockerImage("replicator-runner:latest"))
                    .withNetwork(network)
                    .withNetworkAliases("replicator")
                    .withClasspathResourceMapping(
                        "replicator-conf.yaml",
                        "/replicator/replicator-conf.yaml",
                        BindMode.READ_ONLY)
                    .withCommand(startReplication)
            ;

        }

        public void start() {
            mysql.start();
            zookeeper.start();
            kafka.start();
            graphite.start();
            replicator.start();
        }

        public void shutdown() {
            replicator.start();
            graphite.stop();
            kafka.start();
            zookeeper.start();
            mysql.stop();
        }

        public String getMySqlIP() {
            return mysql.getContainerIpAddress();
        }

        public Integer getMySqlPort() {
            return mysql.getMappedPort(3306);
        }


        public String getGraphitelIP() {
            return graphite.getContainerIpAddress();
        }

        public Integer getGraphitePort() {
            return graphite.getMappedPort(80);
        }
    }

    @Test
    public void testMySQL2KafkaPipeline() throws InterruptedException {

        Pipeline pipeline = new Pipeline();
        pipeline.start();

        logger.info("Pipeline started");
        logger.info("MySQL is exposed at " + pipeline.getMySqlIP() + ":" + pipeline.getMySqlPort());
        logger.info("Graphite is exposed at " + pipeline.getGraphitelIP() + ":" + pipeline.getGraphitePort());

        def urlReplicant    = 'jdbc:mysql://' + pipeline.getMySqlIP() + ":" + pipeline.getMySqlPort() + '/test'
        def urlActiveSchema = 'jdbc:mysql://' + pipeline.getMySqlIP() + ":" + pipeline.getMySqlPort() + '/test_active_schema'

        logger.info("jdbc url: " + urlReplicant)

        def dbReplicant = [
                url:urlReplicant,
                user:'root',
                password:'mysqlPass',
                driver:'com.mysql.jdbc.Driver'
        ]

        def dbActiveSchema = [
                url:urlActiveSchema,
                user:'root',
                password:'mysqlPass',
                driver:'com.mysql.jdbc.Driver'
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
            ['tNMeE','686140','665726','PZBAAQSVoSxxFassEAQ'],
            ['QrTSd','1049668','49070','cvjIXQiWLegvLs kXaKH'],
            ['xzbTw','4484536','437616','pjFNkiZExAiHkKiJePMp'],
            ['CIael','2872792','978231','RWURqZcnAGwQfRSisYcr'],
            ['Cwd j','2071578','260864','jrotGtNYxRmpIKJbAEPd']
        ]

        testRows.each{
            row -> try {
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

//        // SELECT
        def resultSet = []
        replicant.eachRow('select * from sometable') {
            row ->
                resultSet.add([
                        pk_part_1: row.pk_part_1,
                        pk_part_2: row.pk_part_2,
                        randomInt: row.randomInt,
                        randomVarchar: row.randomVarchar
                ])
        }

        logger.info("retrieved from MySQL: " + prettyPrint(toJson(resultSet)))

        replicant.close()
        activeSchema.close()

        // TODO: read from Kafka and compare

        Thread.sleep(1000000);
    }

}

