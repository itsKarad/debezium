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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
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
    @FixFor("debezium/dbz#3146")
    @SuppressWarnings("unchecked")
    void heartbeatWindowIterationIsSerializedWithExecuteSnapshotSignal() throws Exception {
        final PostgresConnectorConfig connectorConfig = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.READ_ONLY_CONNECTION, true)
                .build());
        final PostgresOffsetContext.Loader offsetLoader = new PostgresOffsetContext.Loader(connectorConfig);
        final PostgresOffsetContext heartbeatOffset = offsetLoader.load(Map.of(SourceInfo.TXID_KEY, 49L));
        final PostgresReadOnlyIncrementalSnapshotContext<TableId> heartbeatContext = (PostgresReadOnlyIncrementalSnapshotContext<TableId>) heartbeatOffset
                .getIncrementalSnapshotContext();
        heartbeatContext.addDataCollectionNamesToSnapshot("initial", List.of("public.initial_table"), List.of(), "");
        heartbeatContext.startNewChunk();
        heartbeatContext.setLowWatermark(new PgSnapshot(40L, 45L, Set.of()));
        heartbeatContext.setHighWatermark(new PgSnapshot(50L, 60L, Set.of()));

        // Separate offsets model streaming and signal callbacks. The signal callback reaches readChunk on this source and mutates its shared window.
        final PostgresOffsetContext signalOffset = offsetLoader.load(Map.of(SourceInfo.TXID_KEY, 49L));
        final PostgresPartition partition = new PostgresPartition("test", "testdb");
        when(schema.tableIds()).thenReturn(Set.of());

        final NotificationService<PostgresPartition, PostgresOffsetContext> notifications = new NotificationService<>(List.of(), connectorConfig, null, null);
        final TestSource testSource = new TestSource(connectorConfig, jdbcConnection, dispatcher, schema, clock,
                progressListener, dataChangeEventListener, notifications, heartbeatOffset, signalOffset);
        testSource.init(partition, heartbeatOffset);
        testSource.addWindowRow(1);
        testSource.addWindowRow(2);

        final ExecutorService executor = Executors.newFixedThreadPool(2);
        final AtomicReference<Thread> heartbeatWorker = new AtomicReference<>();
        final AtomicReference<Thread> signalWorker = new AtomicReference<>();
        final CountDownLatch signalAttempted = new CountDownLatch(1);
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final SignalPayload<PostgresPartition> signalPayload = new SignalPayload<>(partition, "signal", "execute-snapshot", null,
                signalOffset, Map.of());
        final SnapshotConfiguration snapshotConfiguration = SnapshotConfiguration.Builder.builder()
                .dataCollections(List.of("public.signal_table"))
                .surrogateKey("")
                .build();

        try {
            final Future<?> heartbeat = executor.submit(() -> {
                heartbeatWorker.set(Thread.currentThread());
                testSource.processHeartbeat(partition, heartbeatOffset);
                return null;
            });
            assertThat(testSource.windowIterationStarted.await(5, TimeUnit.SECONDS)).isTrue();

            final Future<?> signal = executor.submit(() -> {
                signalWorker.set(Thread.currentThread());
                signalAttempted.countDown();
                testSource.addDataCollectionNamesToSnapshot(signalPayload, snapshotConfiguration);
                return null;
            });
            assertThat(signalAttempted.await(5, TimeUnit.SECONDS)).isTrue();

            Awaitility.await().atMost(5, TimeUnit.SECONDS).until(() -> {
                final Thread signalThread = signalWorker.get();
                final Thread heartbeatThread = heartbeatWorker.get();
                if (signalThread == null || heartbeatThread == null || signalThread.getState() != Thread.State.BLOCKED) {
                    return false;
                }

                final ThreadInfo threadInfo = threadMXBean.getThreadInfo(signalThread.getId());
                return threadInfo != null && threadInfo.getLockOwnerId() == heartbeatThread.getId();
            });
            assertThat(testSource.signalChunkStarted.getCount()).isEqualTo(1);

            testSource.resumeWindowIteration.countDown();
            heartbeat.get(5, TimeUnit.SECONDS);
            signal.get(5, TimeUnit.SECONDS);
            assertThat(testSource.signalChunkStarted.getCount()).isZero();
            assertThat(testSource.windowSize()).isEqualTo(1);
        }
        finally {
            testSource.resumeWindowIteration.countDown();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static final class TestSource extends PostgresReadOnlyIncrementalSnapshotChangeEventSource<PostgresPartition> {

        private static final Schema WINDOW_KEY_SCHEMA = SchemaBuilder.struct().field("id", Schema.INT32_SCHEMA).build();

        private final OffsetContext heartbeatOffset;
        private final OffsetContext signalOffset;
        private final AtomicInteger sentEvents = new AtomicInteger();
        private final AtomicInteger heartbeatChunkReads = new AtomicInteger();
        private final CountDownLatch windowIterationStarted = new CountDownLatch(1);
        private final CountDownLatch resumeWindowIteration = new CountDownLatch(1);
        private final CountDownLatch signalChunkStarted = new CountDownLatch(1);

        private TestSource(PostgresConnectorConfig config, PostgresConnection jdbcConnection, EventDispatcher<PostgresPartition, TableId> dispatcher,
                           PostgresSchema schema, Clock clock, SnapshotProgressListener<PostgresPartition> progressListener,
                           DataChangeEventListener<PostgresPartition> dataChangeEventListener,
                           NotificationService<PostgresPartition, ? extends OffsetContext> notificationService,
                           OffsetContext heartbeatOffset, OffsetContext signalOffset) {
            super(config, jdbcConnection, dispatcher, schema, clock, progressListener, dataChangeEventListener, notificationService);
            this.heartbeatOffset = heartbeatOffset;
            this.signalOffset = signalOffset;
        }

        @Override
        protected void preIncrementalSnapshotStart() {
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void readChunk(PostgresPartition partition, OffsetContext offsetContext) {
            if (offsetContext == signalOffset) {
                signalChunkStarted.countDown();
                window.put(windowKey(3), new Object[0]);
            }
            else if (offsetContext == heartbeatOffset && heartbeatChunkReads.incrementAndGet() > 1) {
                final PostgresReadOnlyIncrementalSnapshotContext<TableId> snapshotContext = (PostgresReadOnlyIncrementalSnapshotContext<TableId>) context;
                snapshotContext.setLowWatermark(new PgSnapshot(41L, 46L, Set.of()));
                snapshotContext.setHighWatermark(new PgSnapshot(51L, 61L, Set.of()));
            }
        }

        @Override
        protected void sendEvent(PostgresPartition partition, EventDispatcher<PostgresPartition, TableId> dispatcher,
                                 OffsetContext offsetContext, Object[] row)
                throws InterruptedException {
            if (sentEvents.getAndIncrement() == 0) {
                windowIterationStarted.countDown();
                if (!resumeWindowIteration.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting to resume heartbeat window iteration");
                }
            }
        }

        private void addWindowRow(int id) {
            window.put(windowKey(id), new Object[0]);
        }

        private int windowSize() {
            return window.size();
        }

        private static Struct windowKey(int id) {
            return new Struct(WINDOW_KEY_SCHEMA).put("id", id);
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
