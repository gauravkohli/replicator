package com.booking.replication.spec

import com.booking.replication.ReplicatorIntegrationTest
import groovy.sql.Sql;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes

import static groovy.json.JsonOutput.prettyPrint
import static groovy.json.JsonOutput.toJson

class BasicHBaseTransmitSpec implements ReplicatorIntegrationTest {

    private String HBASE_COLUMN_FAMILY_NAME = "d"

     Sql getReplicantSql(boolean autoCommit) { // TODO: move to ServiceProvider in common; split common to test-utils and common

        def urlReplicant = 'jdbc:mysql://' + getContainerIpAddress() + ":" + getMappedPort(3306) + '/test'
//        logger.debug("jdbc url: " + urlReplicant);
        def dbReplicant = [
                url     : urlReplicant,
                user    : 'root',
                password: 'replicator',
                driver  : 'com.mysql.jdbc.Driver'
        ]
        def replicant = Sql.newInstance(
                dbReplicant.url,
                dbReplicant.user,
                dbReplicant.password,
                dbReplicant.driver
        );
        replicant.connection.autoCommit = autoCommit
        return replicant;
    }


    @Override
    void doMySQLOps() {
        def replicantMySQLHandle = pipeline.mysql.getReplicantSql(
                false // <- autoCommit
        )

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

        replicantMySQLHandle.execute(sqlCreate);
        replicantMySQLHandle.commit();

        // INSERT
        def testRows = [
                ['A', '1', '665726', 'PZBAAQSVoSxxFassQEAQ'],
                ['B', '2', '490705', 'cvjIXQiWLegvLs kXaKH'],
                ['C', '3', '437616', 'pjFNkiZExAiHkKiJePMp'],
                ['D', '4', '537616', 'SjFNkiZExAiHkKiJePMp'],
                ['E', '5', '637616', 'ajFNkiZExAiHkKiJePMp']
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

                    replicantMySQLHandle.execute(sqlString)
                    replicantMySQLHandle.commit()
                } catch (Exception ex) {
                    replicantMySQLHandle.rollback()
                }
        }

//        // SELECT CHECK
//        def resultSet = []
//        replicantMySQLHandle.eachRow('select * from sometable') {
//            row ->
//                resultSet.add([
//                        pk_part_1    : row.pk_part_1,
//                        pk_part_2    : row.pk_part_2,
//                        randomInt    : row.randomInt,
//                        randomVarchar: row.randomVarchar
//                ])
//        }
//        print("retrieved from MySQL: " + prettyPrint(toJson(resultSet)))

        replicantMySQLHandle.close()
    }

    @Override
    boolean retrievedEqualsExpected(Object expected, Object retrieved) {
        return true // TODO: implement comparison
    }

    @Override
    List<String> getExpected() {
        return [
                "0d61f837;C;3|d:pk_part_1|C",
                "0d61f837;C;3|d:pk_part_2|3",
                "0d61f837;C;3|d:randomint|437616",
                "0d61f837;C;3|d:randomvarchar|pjFNkiZExAiHkKiJePMp",
                "0d61f837;C;3|d:row_status|I",
                "3a3ea00c;E;5|d:pk_part_1|E",
                "3a3ea00c;E;5|d:pk_part_2|5",
                "3a3ea00c;E;5|d:randomint|637616",
                "3a3ea00c;E;5|d:randomvarchar|ajFNkiZExAiHkKiJePMp",
                "3a3ea00c;E;5|d:row_status|I",
                "7fc56270;A;1|d:pk_part_1|A",
                "7fc56270;A;1|d:pk_part_2|1",
                "7fc56270;A;1|d:randomint|665726",
                "7fc56270;A;1|d:randomvarchar|PZBAAQSVoSxxFassQEAQ",
                "7fc56270;A;1|d:row_status|I",
                "9d5ed678;B;2|d:pk_part_1|B",
                "9d5ed678;B;2|d:pk_part_2|2",
                "9d5ed678;B;2|d:randomint|490705",
                "9d5ed678;B;2|d:randomvarchar|cvjIXQiWLegvLs kXaKH",
                "9d5ed678;B;2|d:row_status|I",
                "f623e75a;D;4|d:pk_part_1|D",
                "f623e75a;D;4|d:pk_part_2|4",
                "f623e75a;D;4|d:randomint|537616",
                "f623e75a;D;4|d:randomvarchar|SjFNkiZExAiHkKiJePMp",
                "f623e75a;D;4|d:row_status|I"
        ]
    }

     @Override
     Object retrieveReplicatedData() throws IOException {

        String tableName = "organisms"
        def data = new HashMap<>()
        try {
            // config
            Configuration config = HBaseConfiguration.create()
            Connection connection = ConnectionFactory.createConnection(config)
            Table table = connection.getTable(TableName.valueOf(Bytes.toBytes(tableName)))

            // read
            Scan scan = new Scan()
            scan.setMaxVersions(1000)
            ResultScanner scanner = table.getScanner(scan)
            for (Result row : scanner) {
                CellScanner cs =  row.cellScanner()
                while (cs.advance()) {
                    Cell cell = cs.current()
                    String columnName = Bytes.toString(cell.getQualifier())

                    if (data[tableName] == null) {
                        data[tableName] = new HashMap<>();
                    }
                    if (data[tableName][columnName] == null) {
                        data[tableName][columnName] = new HashMap<>()
                    }
                    data.get(tableName).get(columnName).put(
                        cell.getTimestamp(), Bytes.toString(cell.getValue())
                    )
                }
            }
        } catch (IOException e) {
            e.printStackTrace()
        }
        return data
    }
}
