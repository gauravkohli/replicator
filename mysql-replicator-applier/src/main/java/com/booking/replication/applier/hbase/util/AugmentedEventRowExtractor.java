package com.booking.replication.applier.hbase.util;

import com.booking.replication.augmenter.model.event.AugmentedEvent;
import com.booking.replication.augmenter.model.event.DeleteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.UpdateRowsAugmentedEventData;
import com.booking.replication.augmenter.model.event.WriteRowsAugmentedEventData;
import com.booking.replication.augmenter.model.row.AugmentedRow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AugmentedEventRowExtractor {

    public static List<AugmentedRow> extractAugmentedRows(AugmentedEvent augmentedEvent) {

        List<AugmentedRow> augmentedRows = new ArrayList<>();

        switch (augmentedEvent.getHeader().getEventType()) {

            case WRITE_ROWS:
                WriteRowsAugmentedEventData writeRowsAugmentedEventData =
                        ((WriteRowsAugmentedEventData) augmentedEvent.getData());

                Collection<AugmentedRow> extractedAugmentedRowsFromInsert = writeRowsAugmentedEventData.getAugmentedRows();

                augmentedRows.addAll(extractedAugmentedRowsFromInsert);

                break;

            case UPDATE_ROWS:
                UpdateRowsAugmentedEventData updateRowsAugmentedEventData =
                        ((UpdateRowsAugmentedEventData) augmentedEvent.getData());

                Collection<AugmentedRow> extractedAugmentedRowsFromUpdate =
                        updateRowsAugmentedEventData.getAugmentedRows();

                augmentedRows.addAll(extractedAugmentedRowsFromUpdate);

                break;

            case DELETE_ROWS:
                DeleteRowsAugmentedEventData deleteRowsAugmentedEventData =
                        ((DeleteRowsAugmentedEventData) augmentedEvent.getData());

                Collection<AugmentedRow> extractedAugmentedRowsFromDelete =
                        deleteRowsAugmentedEventData.getAugmentedRows();

                augmentedRows.addAll(extractedAugmentedRowsFromDelete);

                break;

            default:
                break;
        }
        return augmentedRows;
    }
}