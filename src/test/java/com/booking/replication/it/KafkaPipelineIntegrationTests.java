package com.booking.replication.it;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.slf4j.Logger;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.RemoteDockerImage;
import static org.junit.Assert.assertEquals;

import lombok.NonNull;

/**
 * Created by bdevetak on 2/8/18.
 */
public class KafkaPipelineIntegrationTests {

    private static final Logger logger = LoggerFactory.getLogger(KafkaPipelineIntegrationTests.class);

    @ClassRule
    public static MySQLContainer mysqlCustomConfig = new MySQLContainer("mysql:5.6");

    @ClassRule
    public static GenericContainer zookeeper = new GenericContainer("zookeeper:3.4");

    @ClassRule
    public static GenericContainer kafka = new GenericContainer("wurstmeister/kafka:1.0.0");

    @ClassRule
    public static GenericContainer replicator = new GenericContainer(
            new RemoteDockerImage("replicator-runner:latest")
    );

//    @Test
//    public void testMySQL2KafkaPipeline(){
//
//        String runReplicatorCommand = "java -jar /replicator/mysql-replicator.jar \\\n" +
//                "    --parser bc \\\n" +
//                "    --applier kafka \\\n" +
//                "    --schema test \\\n" +
//                "    --binlog-filename binlog.000001 \\\n" +
//                "    --config-path replicator-conf.yaml";
//
//    }

    @Test
    public void testSimple() throws SQLException {

        MySQLContainer mysql = (MySQLContainer) new MySQLContainer()
                .withLogConsumer(new Slf4jLogConsumer(logger));
        mysql.start();

        try {
            ResultSet resultSet = performQuery(mysql, "SELECT 1");
            int resultSetInt = resultSet.getInt(1);

            assertEquals("A basic SELECT query succeeds", 1, resultSetInt);
        } finally {
            mysql.stop();
        }
    }

    @NonNull
    protected ResultSet performQuery(MySQLContainer containerRule, String sql) throws SQLException {

        HikariConfig hikariConfig = new HikariConfig();

        hikariConfig.setJdbcUrl(containerRule.getJdbcUrl());
        hikariConfig.setUsername(containerRule.getUsername());
        hikariConfig.setPassword(containerRule.getPassword());

        HikariDataSource ds = new HikariDataSource(hikariConfig);

        Statement statement = ds.getConnection().createStatement();

        statement.execute(sql);

        ResultSet resultSet = statement.getResultSet();

        resultSet.next();

        return resultSet;
    }

}
