/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mysql;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.connector.binlog.AbstractBinlogConnectorIT;
import io.debezium.connector.binlog.BinlogConnectorConfig;
import io.debezium.connector.binlog.util.TestHelper;
import io.debezium.connector.binlog.util.UniqueDatabase;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.relational.RelationalSnapshotChangeEventSource;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;

public class MySqlSnapshotPositionOrderIT extends AbstractBinlogConnectorIT<MySqlConnector> implements MySqlCommon {

    private static final Path DB_HISTORY_PATH = Files.createTestingPath("file-db-history-2942.txt").toAbsolutePath();
    private final UniqueDatabase DATABASE = TestHelper.getUniqueDatabase("simulation", "innodb_mvcc").withDbHistoryPath(DB_HISTORY_PATH);

    @BeforeEach
    void beforeEach() {
        stopConnector();
        DATABASE.createAndInitialize();
        initializeConnectorTestFramework();
        Files.delete(DB_HISTORY_PATH);
    }

    @AfterEach
    void afterEach() {
        try {
            stopConnector();
        }
        finally {
            Files.delete(DB_HISTORY_PATH);
        }
    }

    @Test
    @FixFor("debezium/dbz#2942")
    public void shouldRecordSnapshotPositionBeforePooledConnectionReadView() throws Exception {
        final List<ILoggingEvent> orderedEvents = new CopyOnWriteArrayList<>();
        try (OrderedLogInterceptor snapshotLogs = new OrderedLogInterceptor(MySqlSnapshotChangeEventSource.class.getName(), orderedEvents);
                OrderedLogInterceptor relationalSnapshotLogs = new OrderedLogInterceptor(RelationalSnapshotChangeEventSource.class.getName(), orderedEvents);
                OrderedLogInterceptor connectionLogs = new OrderedLogInterceptor(JdbcConnection.class.getName(), orderedEvents)) {
            snapshotLogs.setLoggerLevel(MySqlSnapshotChangeEventSource.class, Level.INFO);
            connectionLogs.setLoggerLevel(JdbcConnection.class, Level.TRACE);

            start(getConnectorClass(), DATABASE.defaultConfig()
                    .with(BinlogConnectorConfig.SNAPSHOT_MAX_THREADS, 2)
                    .with(MySqlConnectorConfig.SNAPSHOT_LOCKING_MODE, MySqlConnectorConfig.SnapshotLockingMode.NONE)
                    .build());

            waitForSnapshotToBeCompleted(getConnectorName(), DATABASE.getServerName());

            final int positionCapture = snapshotLogs.indexOf(MySqlSnapshotChangeEventSource.class.getName(),
                    "Read binlog position of MySQL primary server");
            final int tableDiscovery = relationalSnapshotLogs.indexOf(RelationalSnapshotChangeEventSource.class.getName(),
                    "Snapshot step 2 - Determining captured tables");
            final String firstPooledRead = "SELECT * FROM `" + DATABASE.getDatabaseName() + "`.`test_table` LIMIT 1";
            final int pooledReadView = connectionLogs.indexOfStatement(JdbcConnection.class.getName(), firstPooledRead);
            final int poolCreated = relationalSnapshotLogs.indexOf(RelationalSnapshotChangeEventSource.class.getName(),
                    "Created connection pool with 2 threads");

            assertThat(positionCapture).isNotNegative();
            assertThat(tableDiscovery).isNotNegative();
            assertThat(pooledReadView).isNotNegative();
            assertThat(connectionLogs.countStatements(JdbcConnection.class.getName(), firstPooledRead)).isEqualTo(1L);
            assertThat(positionCapture).isLessThan(tableDiscovery);
            assertThat(poolCreated).isGreaterThan(pooledReadView);
            assertThat(positionCapture).isLessThan(pooledReadView);
        }
    }

    private static class OrderedLogInterceptor extends LogInterceptor {

        private final List<ILoggingEvent> orderedEvents;

        private OrderedLogInterceptor(String loggerName, List<ILoggingEvent> orderedEvents) {
            super(loggerName);
            this.orderedEvents = orderedEvents;
        }

        @Override
        protected void append(ILoggingEvent event) {
            orderedEvents.add(event);
        }

        private int indexOf(String loggerName, String message) {
            for (int i = 0; i < orderedEvents.size(); i++) {
                final ILoggingEvent event = orderedEvents.get(i);
                if (loggerName.equals(event.getLoggerName()) && event.getFormattedMessage().contains(message)) {
                    return i;
                }
            }
            return -1;
        }

        private int indexOfStatement(String loggerName, String statement) {
            for (int i = 0; i < orderedEvents.size(); i++) {
                final ILoggingEvent event = orderedEvents.get(i);
                if (loggerName.equals(event.getLoggerName()) && event.getFormattedMessage()
                        .equals("executing '" + statement + "'")) {
                    return i;
                }
            }
            return -1;
        }

        private long countStatements(String loggerName, String statement) {
            return orderedEvents.stream()
                    .filter(event -> loggerName.equals(event.getLoggerName()))
                    .filter(event -> event.getFormattedMessage()
                            .equals("executing '" + statement + "'"))
                    .count();
        }
    }
}
