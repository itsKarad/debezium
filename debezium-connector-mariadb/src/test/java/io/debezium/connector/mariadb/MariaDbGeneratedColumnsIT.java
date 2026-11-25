/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.AbstractBinlogConnectorIT;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogConnectorConfig.SnapshotMode;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;

public class MariaDbGeneratedColumnsIT extends AbstractBinlogConnectorIT<MariaDbConnector> implements MariaDbCommon {

    private static final Path SCHEMA_HISTORY_PATH = Files.createTestingPath("file-schema-history-generated-columns.txt").toAbsolutePath();
    private static final String GENERATED_AT_END = "generated_at_end";
    private static final String GENERATED_INTERLEAVED = "generated_interleaved";

    private final UniqueDatabase database = TestHelper.getUniqueDatabase("mariadbGeneratedColumns", "generated_columns_test")
            .withDbHistoryPath(SCHEMA_HISTORY_PATH);

    @BeforeEach
    public void beforeEach() {
        stopConnector();
        database.create();
        executeStatements("mysql", "SET GLOBAL binlog_row_image='FULL'");
        executeStatements(database.getDatabaseName(),
                "CREATE TABLE " + GENERATED_AT_END + " (id INT PRIMARY KEY, a INT, v INT AS (a + 1) VIRTUAL, s INT AS (a + 2) PERSISTENT)",
                "CREATE TABLE " + GENERATED_INTERLEAVED + " (id INT PRIMARY KEY, a INT, v INT AS (a + 1) VIRTUAL, b INT, s INT AS (b + 2) PERSISTENT)");
        initializeConnectorTestFramework();
        Files.delete(SCHEMA_HISTORY_PATH);
    }

    @AfterEach
    public void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(SCHEMA_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("debezium/dbz#2910")
    public void shouldProcessUpdatesAndDeletesWhenGeneratedColumnsAreOmitted() throws InterruptedException {
        final Configuration config = database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, SnapshotMode.INITIAL)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.TOMBSTONES_ON_DELETE, false)
                .build();

        start(getConnectorClass(), config);
        waitForStreamingRunning(getConnectorName(), database.getServerName());

        executeStatements(database.getDatabaseName(),
                "SET SESSION binlog_row_image='FULL'",
                "INSERT INTO " + GENERATED_AT_END + " (id, a) VALUES (1, 1)",
                "INSERT INTO " + GENERATED_INTERLEAVED + " (id, a, b) VALUES (1, 10, 20)");

        SourceRecords inserts = consumeRecordsByTopic(2);
        assertInsert(inserts, GENERATED_AT_END, 1, 1, 2, null, 3);
        assertInsert(inserts, GENERATED_INTERLEAVED, 1, 10, 11, 20, 22);

        executeStatements(database.getDatabaseName(),
                "SET SESSION binlog_row_image='FULL'",
                "UPDATE " + GENERATED_AT_END + " SET a = 2 WHERE id = 1",
                "UPDATE " + GENERATED_INTERLEAVED + " SET a = 11, b = 21 WHERE id = 1");

        SourceRecords updates = consumeRecordsByTopic(2);
        assertUpdate(updates, GENERATED_AT_END, 1, 1, null, null, null, 2, null, null, null);
        assertUpdate(updates, GENERATED_INTERLEAVED, 1, 10, null, 20, null, 11, null, 21, null);

        executeStatements(database.getDatabaseName(),
                "SET SESSION binlog_row_image='FULL'",
                "DELETE FROM " + GENERATED_AT_END + " WHERE id = 1",
                "DELETE FROM " + GENERATED_INTERLEAVED + " WHERE id = 1");

        SourceRecords deletes = consumeRecordsByTopic(2);
        assertDelete(deletes, GENERATED_AT_END, 1, 2, null, null, null);
        assertDelete(deletes, GENERATED_INTERLEAVED, 1, 11, null, 21, null);
    }

    private void assertInsert(SourceRecords records, String table, int id, int a, Integer v, Integer b, Integer s) {
        final Struct after = ((Struct) record(records, table).value()).getStruct("after");
        assertThat(after.getInt32("id")).isEqualTo(id);
        assertThat(after.getInt32("a")).isEqualTo(a);
        assertThat(after.getInt32("v")).isEqualTo(v);
        if (b != null) {
            assertThat(after.getInt32("b")).isEqualTo(b);
        }
        assertThat(after.getInt32("s")).isEqualTo(s);
    }

    private void assertUpdate(SourceRecords records, String table, int id, int beforeA, Integer beforeV, Integer beforeB, Integer beforeS,
                              int afterA, Integer afterV, Integer afterB, Integer afterS) {
        final Struct envelope = (Struct) record(records, table).value();
        final Struct before = envelope.getStruct("before");
        final Struct after = envelope.getStruct("after");
        assertThat(before.getInt32("id")).isEqualTo(id);
        assertThat(before.getInt32("a")).isEqualTo(beforeA);
        assertThat(before.getInt32("v")).isEqualTo(beforeV);
        if (beforeB != null) {
            assertThat(before.getInt32("b")).isEqualTo(beforeB);
        }
        assertThat(before.getInt32("s")).isEqualTo(beforeS);
        assertThat(after.getInt32("id")).isEqualTo(id);
        assertThat(after.getInt32("a")).isEqualTo(afterA);
        assertThat(after.getInt32("v")).isEqualTo(afterV);
        if (afterB != null) {
            assertThat(after.getInt32("b")).isEqualTo(afterB);
        }
        assertThat(after.getInt32("s")).isEqualTo(afterS);
    }

    private void assertDelete(SourceRecords records, String table, int id, int a, Integer v, Integer b, Integer s) {
        final Struct before = ((Struct) record(records, table).value()).getStruct("before");
        assertThat(before.getInt32("id")).isEqualTo(id);
        assertThat(before.getInt32("a")).isEqualTo(a);
        assertThat(before.getInt32("v")).isEqualTo(v);
        if (b != null) {
            assertThat(before.getInt32("b")).isEqualTo(b);
        }
        assertThat(before.getInt32("s")).isEqualTo(s);
    }

    private SourceRecord record(SourceRecords records, String table) {
        return records.recordsForTopic(database.topicForTable(table)).get(0);
    }
}
