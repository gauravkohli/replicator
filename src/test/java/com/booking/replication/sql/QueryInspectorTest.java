package com.booking.replication.sql;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class QueryInspectorTest {

    @Test
    public void makeSureWeCanExtractGtidFromAppropriateBinlogEvents() throws Exception {

        QueryInspector.setIsPseudoGTIDPattern("(?<=_pseudo_gtid_hint__asc\\:)(.{8}\\:.{16}\\:.{8})");

        assertTrue(QueryInspector.isPseudoGTID("use `booking_meta`; drop view if exists `booking_meta`.`_pseudo_gtid_hint__asc:57E404AC:000000002A5B1D64:A4608F96`"));

        assertEquals("57E404AC:000000002A5B1D64:A4608F96",
                QueryInspector.extractPseudoGTID("use `booking_meta`; drop view if exists `booking_meta`.`_pseudo_gtid_hint__asc:57E404AC:000000002A5B1D64:A4608F96`"));
    }

    @Test
    public void isDDLTemporaryTable() {

        String querySql = "CREATE TEMPORARY TABLE core.my_tmp_table \n" +
                "(INDEX my_index_name (tag, time), UNIQUE my_unique_index_name (order_number))\n" +
                "SELECT * FROM core.my_big_table\n" +
                "WHERE my_val = 1";

        assertTrue(QueryInspector.isDDLTemporaryTable(querySql));

    }

    @Test
    public void isNotDDLTemporaryTAble() {
        String querySql = "CREATE TABLE core.my_tmp_table \n" +
                "(INDEX my_index_name (tag, time), UNIQUE my_unique_index_name (order_number))\n" +
                "SELECT * FROM core.my_big_table\n" +
                "WHERE my_val = 1";

        assertFalse(QueryInspector.isDDLTemporaryTable(querySql));

    }

    @Test
    public void isDDLTemporaryTableWithComment(){

        String querySQL = "/* Stuff to comment */ CREATE TEMPORARY TABLE core.my_tmp_table \n" +
                "(INDEX my_index_name (tag, time), UNIQUE my_unique_index_name (order_number))\n" +
                "SELECT * FROM core.my_big_table\n" +
                "WHERE my_val = 1 ";

        assertTrue(QueryInspector.isDDLTemporaryTable(querySQL));
    }

    @Test
    public void isDDLDefiner() {
        String querySQL = "CREATE DEFINER=user@example.com";

        assertTrue(QueryInspector.isDDLDefiner(querySQL));
    }

    @Test
    public void isDDLView() {

        String querySQL = "/* Stuff to comment */CREATE VIEW core.my_tmp_table \n" +
                "(INDEX my_index_name (tag, time), UNIQUE my_unique_index_name (order_number))\n" +
                "SELECT * FROM core.my_big_table\n" +
                "WHERE my_val = 1 ";

        assertTrue(QueryInspector.isDDLView(querySQL));
    }

    @Test
    public void isAnalyze() {

        String querySQL = "ANALYZE TABLE Employee";

        assertTrue(QueryInspector.isAnalyze(querySQL));
    }

}