/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;

import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.BinlogReaderBufferIT;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;

/**
 * @author Chris Cranford
 */
public class MariaDbBinlogReaderBufferIT extends BinlogReaderBufferIT<MariaDbConnector> implements MariaDbCommon {

    @Test
    @FixFor("debezium/dbz#3000")
    void shouldReplayAllRowsOfLargeGtidTransactionAndKeepStreaming() throws Exception {
        final var historyPath = Files.createTestingPath("file-schema-history-buffer-rewind.txt").toAbsolutePath();
        Files.delete(historyPath);
        final UniqueDatabase database = TestHelper.getUniqueDatabase("bufferRewind", "connector_test")
                .withDbHistoryPath(historyPath);
        database.createAndInitialize();
        final Configuration config = database.defaultConfig()
                .with(BinlogConnectorConfig.SNAPSHOT_MODE, BinlogConnectorConfig.SnapshotMode.NO_DATA)
                .with(BinlogConnectorConfig.INCLUDE_SCHEMA_CHANGES, false)
                .with(BinlogConnectorConfig.BUFFER_SIZE_FOR_BINLOG_READER, 9)
                .build();

        start(MariaDbConnector.class, config);
        waitForStreamingRunning("mariadb", database.getServerName());

        try (var databaseConnection = getTestDatabaseConnection(database.getDatabaseName());
                var connection = databaseConnection.connect();
                var insert = connection.connection().prepareStatement("INSERT INTO customers (first_name, last_name, email) VALUES ('f', 'l', ?)")) {
            connection.setAutoCommit(false);
            for (int i = 0; i < 40; i++) {
                insert.setString(1, Integer.toString(i));
                insert.executeUpdate();
            }
            connection.connection().commit();
            insert.setString(1, "40");
            insert.executeUpdate();
            connection.connection().commit();
        }

        final var records = consumeRecordsByTopic(41);
        assertThat(records.recordsForTopic(database.topicForTable("customers")))
                .extracting(record -> ((Struct) record.value()).getStruct("after").getString("email"))
                .containsExactlyElementsOf(IntStream.range(0, 41).mapToObj(Integer::toString).toList());
        assertNoRecordsToConsume();
    }
}
