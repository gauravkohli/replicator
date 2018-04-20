package com.booking.replication.augmenter.active.schema.augmented;

import com.booking.replication.augmenter.active.schema.augmented.active.schema.TableSchemaVersion;

import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public interface AugmentedRow {
    Map<String, Map<String, String>> getEventColumns();

    TableSchemaVersion getTableSchemaVersion();

    List<String> getPrimaryKeyColumns();
}