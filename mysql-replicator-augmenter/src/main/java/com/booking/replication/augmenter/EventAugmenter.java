package com.booking.replication.augmenter;

import com.booking.replication.augmenter.active.schema.ActiveSchemaVersion;
import com.booking.replication.augmenter.exception.TableMapException;
import com.booking.replication.model.RawEvent;
import com.booking.replication.augmenter.transaction.TransactionEventData;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.URISyntaxException;
import java.sql.SQLException;


public class EventAugmenter implements Augmenter {
    public final static String UUID_FIELD_NAME = "_replicator_uuid";
    public final static String XID_FIELD_NAME = "_replicator_xid";

    private ActiveSchemaVersion activeSchemaVersion;
    private final boolean applyUuid;
    private final boolean applyXid;

    private static final Logger LOGGER = LogManager.getLogger(EventAugmenter.class);

    public EventAugmenter(ActiveSchemaVersion asv, boolean applyUuid, boolean applyXid) throws SQLException, URISyntaxException {
        activeSchemaVersion = asv;
        this.applyUuid = applyUuid;
        this.applyXid = applyXid;
    }


    public RawEvent mapDataEventToSchema(
            RawEvent abstractRowRawEvent,
            TransactionEventData currentTransaction
        ) throws Exception {

        RawEvent au = null;

        switch (abstractRowRawEvent.getHeader().getEventType()) {
            case UPDATE_ROWS:
                break;

            case WRITE_ROWS:
                break;

            case DELETE_ROWS:
                break;

            default:
                throw new TableMapException("RBR event type expected! Received type: " +
                        abstractRowRawEvent.getHeader().getEventType().toString(), abstractRowRawEvent
                );
        }

        if (au == null) {
            throw new TableMapException("Augmented event ended up as null - something went wrong!", abstractRowRawEvent);
        }

        return au;
    }

    @Override
    public RawEvent apply(RawEvent rawEvent) {
        return rawEvent;
//        RawEvent augmentedEvent = null;
//        try {
//            augmentedEvent = mapDataEventToSchema(rawEvent, null);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return augmentedEvent;
    }
}