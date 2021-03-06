package com.booking.replication.applier.hbase;

import com.booking.replication.applier.Applier;
import com.booking.replication.applier.hbase.schema.HBaseSchemaManager;
import com.booking.replication.applier.hbase.schema.SchemaTransitionException;
import com.booking.replication.applier.hbase.writer.HBaseApplierWriter;
import com.booking.replication.applier.hbase.writer.HBaseTimeMachineWriter;
import com.booking.replication.augmenter.model.event.*;

import com.booking.replication.augmenter.model.schema.SchemaSnapshot;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class HBaseApplier implements Applier {

    private static final Logger LOG = LogManager.getLogger(com.booking.replication.applier.hbase.HBaseApplier.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private int FLUSH_BUFFER_SIZE       = 5;
    private int BUFFER_FLUSH_TIME_LIMIT = 5;

    private HBaseSchemaManager hbaseSchemaManager;
    private HBaseApplierWriter hBaseApplierWriter;

    private Map<String, Object> configuration;

    org.apache.hadoop.conf.Configuration hbaseConfig;

    public interface Configuration {
        String HBASE_ZOOKEEPER_QUORUM   = "applier.hbase.zookeeper.quorum";
        String REPLICATED_SCHEMA_NAME   = "applier.hbase.sourcedb.name";
        String TARGET_NAMESPACE         = "applier.hbase.destination.namespace";
        String SCHEMA_HISTORY_NAMESPACE = "applier.hbase.schema.history.namespace";
        String INITIAL_SNAPSHOT_MODE    = "applier.hbase.initial.snapshot";
        String HBASE_USE_SNAPPY         = "applier.hbase.snappy";
        String DRYRUN                   = "applier.hbase.dryrun";
        String PAYLOAD_TABLE_NAME       = "applier.hbase.payload.table.name";
    }

    @SuppressWarnings("unused")
    public HBaseApplier(Map<String, Object> configuration) {

        this.configuration = configuration;

        hbaseConfig = getHBaseConfig(configuration);

        try {
            hbaseSchemaManager = new HBaseSchemaManager(configuration);
            LOG.info("Created HBaseSchemaManager.");

            hBaseApplierWriter = new HBaseTimeMachineWriter(hbaseConfig, hbaseSchemaManager,configuration);
            LOG.info("Created HBaseApplierWriter.");

        } catch (IOException | NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
    }

    private  org.apache.hadoop.conf.Configuration getHBaseConfig(Map<String, Object> configuration) {
        // TODO: read BigTable specifics from configuration & setup hbaseConfig accordingly
        return HBaseConfiguration.create();
    }

    /**
     * Logic of Operation:
     * <p>
     * The Streams implementation runs multiple threads and will partition the
     * tasks by transaction id.
     * <p>
     * The Augmenter will send grouped transactions to the Applier.apply(). In case of
     * large transactions it will send lists of 1000 rows.
     * <p>
     * The apply() method returns Boolean. If it returns true, this is a signal for
     * Coordinator that rows are committed. In case of small transactions, applier
     * will internally buffer them and apply will return false until buffer is
     * large enough. Once buffer is large enough it will flushTransactionBuffer the buffer and
     * return true (on success).
     * <p>
     * In short:
     * - return false means that list of rows is buffered
     * - return true means that the list of rows and/or buffered data is markedForCommit.
     * It also means that the safe checkpoint can be advanced forward.
     * - IOException means that all write attempts have failed. This shuts down the whole pipeline.
     * <p>
     * In addition:
     * - In case that flushTransactionBuffer() fails, the retry logic needs to be implemented
     * in the ApplierWriter. The Streams implementation manages threads and
     * groups rows by transactions. Once the transaction batch has been sent
     * to the ApplierWriter it is the responsibility of the
     * ApplierWriter implementation to implement write-retry logic.
     * <p>
     * <p>
     * True/False from apply is only relevant for storing the checkpoint. However, in
     * case of error, apply should throw and Exception and the whole pipeline will shutdown.
     */
    @Override
    public synchronized Boolean apply(Collection<AugmentedEvent> events) {


        checkIfBufferExpired();

        try {
            doSchemaLog(events);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SchemaTransitionException e) {
           throw  new RuntimeException("SchemaTransitionException",e);
        }

        List<AugmentedEvent> dataEvents = extractDataEventsOnly(events);

        List<String> transactionUUIDs = getTransactionUUIDs(events);

        if (transactionUUIDs.size() == 1) {
            String transactionUUID = transactionUUIDs.get(0);
            if ((dataEvents.size() >= FLUSH_BUFFER_SIZE) || hBaseApplierWriter.getTransactionBufferSize(transactionUUID) >= FLUSH_BUFFER_SIZE) {
                hBaseApplierWriter.buffer(transactionUUID, dataEvents);
                boolean s = hBaseApplierWriter.flushTransactionBuffer(transactionUUID);
                if (s) {
                    return true; // <- committed, will advance safe checkpoint
                } else {
                    throw new RuntimeException("Failed to write buffer to HBase");
                }
            } else {
                hBaseApplierWriter.buffer(transactionUUID, dataEvents);
                return false; // buffered
            }
        } else if (transactionUUIDs.size() > 1) {
            // multiple transactions in one list
            for (String transactionUUID : transactionUUIDs) {
                hBaseApplierWriter.buffer(transactionUUID, dataEvents);
            }
            forceFlush();
            return true;
        } else {
            LOG.warn("Empty transaction");
            return false; // treat empty transaction as buffered
        }
    }

    private List<String> getTransactionUUIDs(Collection<AugmentedEvent> events) {
        return events
                .stream()
                .filter(e -> e.getHeader().getEventTransaction() != null)
                .map(e -> e.getHeader().getEventTransaction().getIdentifier())
                .distinct()
                .collect(Collectors.toList());
    }

    private void doSchemaLog(Collection<AugmentedEvent> events) throws IOException, SchemaTransitionException {
        // schema snapshot (DLL log)
        for (AugmentedEvent ev : events) {
            if (ev.getOptionalPayload() != null) {
                LOG.info("AugmentedEvent contains optionalPayload");
                SchemaSnapshot schemaSnapshot = ((SchemaSnapshot)ev.getOptionalPayload());
                hbaseSchemaManager.writeSchemaSnapshot(schemaSnapshot, this.configuration);

                String tableName = schemaSnapshot.getSchemaTransitionSequence().getTableName();

                synchronized (hbaseSchemaManager) {
                    hbaseSchemaManager.createMirroredTableIfNotExists(tableName);
                    LOG.info("created hbase table " + tableName);
                }

                LOG.debug(HBaseApplier.MAPPER.writeValueAsString(schemaSnapshot.getSchemaAfter().getTableSchemaCache()));
            }
        }
    }

    private List<AugmentedEvent> extractDataEventsOnly(Collection<AugmentedEvent> events) {
        // data only (events are already grouped by transaction when passed to apply, so no need to process begin/commit)
        return events.stream().filter(
                ev ->   (ev.getHeader().getEventType() == AugmentedEventType.WRITE_ROWS)
                         ||
                        (ev.getHeader().getEventType() == AugmentedEventType.UPDATE_ROWS)
                         ||
                        (ev.getHeader().getEventType() == AugmentedEventType.DELETE_ROWS)
        ).collect(toList());
    }

    private void checkIfBufferExpired() {
        long now = Instant.now().toEpochMilli();
        if(now - hBaseApplierWriter.getBufferClearTime() > BUFFER_FLUSH_TIME_LIMIT) {
            forceFlush();
        }
    }

    private void checkSchema(Collection<AugmentedEvent> events) {
        // schema check
        for (AugmentedEvent ev : events) {
            try {
                String tableName = extractTableName(ev);
                if (tableName != null) {
                    synchronized (hbaseSchemaManager) {
                        hbaseSchemaManager.createMirroredTableIfNotExists(tableName);
                    }
                }
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            } catch (IOException e) {
                throw  new RuntimeException("Can't create table in HBase");
            }
        }
    }

    private String extractTableName(AugmentedEvent event) {
        String eventTableName;
        switch (event.getHeader().getEventType()) {
            case WRITE_ROWS:
                eventTableName = ((WriteRowsAugmentedEventData) event.getData()).getEventTable().getName();
                break;
            case UPDATE_ROWS:
                eventTableName = ((UpdateRowsAugmentedEventData) event.getData()).getEventTable().getName();
                break;
            case DELETE_ROWS:
                eventTableName = ((DeleteRowsAugmentedEventData) event.getData()).getEventTable().getName();
                break;
            default:
                return null;
        }
        return eventTableName;
    }

    @Override
    public boolean forceFlush() {
        boolean s = false;
        try {
            s = hBaseApplierWriter.forceFlush();
        } catch (IOException e) {
            throw new RuntimeException("forceFlush() failed");
        }
        return s;
    }
}
