/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.doc.FixFor;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.junit.logging.LogInterceptor;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalPayload;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.spi.DataChangeEventListener;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

import ch.qos.logback.classic.Level;

/**
 * Unit tests for {@link PostgresReadOnlyIncrementalSnapshotChangeEventSource}.
 */
public class PostgresReadOnlyIncrementalSnapshotChangeEventSourceTest {

    @Mock
    private RelationalDatabaseConnectorConfig config;
    @Mock
    private PostgresConnection jdbcConnection;
    @Mock
    private PostgresSchema schema;
    @SuppressWarnings("rawtypes")
    @Mock
    private EventDispatcher dispatcher;
    @Mock
    private Clock clock;
    @SuppressWarnings("rawtypes")
    @Mock
    private SnapshotProgressListener progressListener;
    @SuppressWarnings("rawtypes")
    @Mock
    private DataChangeEventListener dataChangeEventListener;
    @SuppressWarnings("rawtypes")
    @Mock
    private NotificationService notificationService;

    private PostgresReadOnlyIncrementalSnapshotChangeEventSource<PostgresPartition> source;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        MockitoAnnotations.openMocks(this);
        source = new PostgresReadOnlyIncrementalSnapshotChangeEventSource<>(
                config, jdbcConnection, dispatcher, schema, clock,
                progressListener, dataChangeEventListener, notificationService);
    }

    @Test
    @FixFor("debezium/dbz#2431")
    void forceTransactionQueryIsRecoveryAware() throws Exception {
        Field field = PostgresReadOnlyIncrementalSnapshotChangeEventSource.class
                .getDeclaredField("FORCE_NEW_TRANSACTION");
        field.setAccessible(true);
        String sql = (String) field.get(null);

        assertThat(sql).contains("pg_is_in_recovery()");
        assertThat(sql).contains("pg_current_xact_id()");
    }

    @Test
    @FixFor("debezium/dbz#2431")
    void forceNewTransactionIdIsNoOpOnHotStandby() throws Exception {
        // Simulate a hot standby: pg_is_in_recovery() = true, so the CASE returns NULL
        stubQueryResult(null);

        LogInterceptor logInterceptor = new LogInterceptor(PostgresReadOnlyIncrementalSnapshotChangeEventSource.class);
        logInterceptor.setLoggerLevel(PostgresReadOnlyIncrementalSnapshotChangeEventSource.class, Level.TRACE);

        assertThatNoException().isThrownBy(this::invokeForceNewTransactionId);
        assertThat(logInterceptor.containsMessage("Skipping transaction ID assignment on hot standby")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#2431")
    void forceNewTransactionIdLogsTransactionOnPrimary() throws Exception {
        // Simulate a primary: pg_is_in_recovery() = false, so a real transaction ID is returned
        stubQueryResult("12345");

        LogInterceptor logInterceptor = new LogInterceptor(PostgresReadOnlyIncrementalSnapshotChangeEventSource.class);
        logInterceptor.setLoggerLevel(PostgresReadOnlyIncrementalSnapshotChangeEventSource.class, Level.TRACE);

        assertThatNoException().isThrownBy(this::invokeForceNewTransactionId);
        assertThat(logInterceptor.containsMessage("Created new transaction ID 12345")).isTrue();
    }

    @Test
    @FixFor("debezium/dbz#3035")
    void streamingMessageWaitsForInitialSignalChunk() throws Exception {
        final PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.READ_ONLY_CONNECTION, true)
                .build());
        final PostgresPartition partition = new PostgresPartition(connectorConfig.getLogicalName(), connectorConfig.databaseName());
        final PostgresOffsetContext offsetContext = new PostgresOffsetContext.Loader(connectorConfig).load(Map.of(SourceInfo.TXID_KEY, 1L));
        when(schema.tableIds()).thenReturn(Set.of());
        final NotificationService<PostgresPartition, OffsetContext> notificationService = new NotificationService<>(List.of(), connectorConfig, null, null);
        final BlockingSnapshotSource blockingSource = new BlockingSnapshotSource(connectorConfig, jdbcConnection, dispatcher, schema, clock,
                progressListener, dataChangeEventListener, notificationService);
        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final AtomicReference<Thread> signalThread = new AtomicReference<>();
        final AtomicReference<Thread> messageThread = new AtomicReference<>();
        final CountDownLatch messageStarted = new CountDownLatch(1);
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

        try {
            final Future<?> signal = executor.submit(() -> {
                signalThread.set(Thread.currentThread());
                blockingSource.addDataCollectionNamesToSnapshot(
                        new SignalPayload<>(partition, "signal-1", "execute-snapshot", null, offsetContext, Map.of()),
                        SnapshotConfiguration.Builder.builder().dataCollections(List.of("s1.test")).surrogateKey("").build());
                return null;
            });
            assertThat(blockingSource.firstChunkStarted.await(5, TimeUnit.SECONDS)).isTrue();

            final Future<?> message = executor.submit(() -> {
                messageThread.set(Thread.currentThread());
                messageStarted.countDown();
                blockingSource.processMessage(partition, new TableId(null, "s1", "test"), null, offsetContext);
                return null;
            });
            assertThat(messageStarted.await(5, TimeUnit.SECONDS)).isTrue();
            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
                if (blockingSource.secondChunkStarted.getCount() == 0) {
                    return true;
                }

                final Thread worker = messageThread.get();
                if (worker.getState() != Thread.State.BLOCKED) {
                    return false;
                }

                final ThreadInfo threadInfo = threadMXBean.getThreadInfo(worker.getId());
                return threadInfo != null && threadInfo.getLockOwnerId() == signalThread.get().getId();
            });
            assertThat(blockingSource.secondChunkStarted.getCount()).isEqualTo(1);

            blockingSource.finishFirstChunk.countDown();
            signal.get(5, TimeUnit.SECONDS);
            message.get(5, TimeUnit.SECONDS);

            assertThat(blockingSource.chunkReadCount.get()).isEqualTo(1);
            assertThat(blockingSource.maximumConcurrentReads.get()).isEqualTo(1);
        }
        finally {
            blockingSource.finishFirstChunk.countDown();
            executor.shutdownNow();
        }
    }

    private static class BlockingSnapshotSource extends PostgresReadOnlyIncrementalSnapshotChangeEventSource<PostgresPartition> {

        private final CountDownLatch firstChunkStarted = new CountDownLatch(1);
        private final CountDownLatch finishFirstChunk = new CountDownLatch(1);
        private final CountDownLatch secondChunkStarted = new CountDownLatch(1);
        private final AtomicInteger activeReads = new AtomicInteger();
        private final AtomicInteger maximumConcurrentReads = new AtomicInteger();
        private final AtomicInteger chunkReadCount = new AtomicInteger();

        BlockingSnapshotSource(RelationalDatabaseConnectorConfig config, JdbcConnection jdbcConnection,
                               EventDispatcher<PostgresPartition, TableId> dispatcher, PostgresSchema schema,
                               Clock clock, SnapshotProgressListener<PostgresPartition> progressListener,
                               DataChangeEventListener<PostgresPartition> dataChangeEventListener,
                               NotificationService<PostgresPartition, ? extends OffsetContext> notificationService) {
            super(config, jdbcConnection, dispatcher, schema, clock, progressListener, dataChangeEventListener, notificationService);
        }

        @Override
        protected void readChunk(PostgresPartition partition, OffsetContext offsetContext) throws InterruptedException {
            final int concurrentReads = activeReads.incrementAndGet();
            maximumConcurrentReads.accumulateAndGet(concurrentReads, Math::max);
            try {
                if (chunkReadCount.incrementAndGet() == 1) {
                    firstChunkStarted.countDown();
                    if (!finishFirstChunk.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("Timed out waiting for the first chunk to finish");
                    }
                    final PostgresReadOnlyIncrementalSnapshotContext<?> context = (PostgresReadOnlyIncrementalSnapshotContext<?>) offsetContext
                            .getIncrementalSnapshotContext();
                    context.startNewChunk();
                    context.setLowWatermark(PgSnapshot.valueOf("1:2:"));
                    context.openWindow(context.currentChunkId());
                }
                else {
                    secondChunkStarted.countDown();
                }
            }
            finally {
                activeReads.decrementAndGet();
            }
        }

        @Override
        protected void sendWindowEvents(PostgresPartition partition, OffsetContext offsetContext) {
        }
    }

    private void stubQueryResult(String txId) throws Exception {
        doAnswer(invocation -> {
            JdbcConnection.ResultSetConsumer consumer = invocation.getArgument(1);
            ResultSet rs = mock(ResultSet.class);
            when(rs.next()).thenReturn(true);
            when(rs.getString(1)).thenReturn(txId);
            consumer.accept(rs);
            return jdbcConnection;
        }).when(jdbcConnection).query(anyString(), any(JdbcConnection.ResultSetConsumer.class));
    }

    private void invokeForceNewTransactionId() throws Exception {
        Method method = PostgresReadOnlyIncrementalSnapshotChangeEventSource.class
                .getDeclaredMethod("forceNewTransactionId");
        method.setAccessible(true);
        method.invoke(source);
    }
}
