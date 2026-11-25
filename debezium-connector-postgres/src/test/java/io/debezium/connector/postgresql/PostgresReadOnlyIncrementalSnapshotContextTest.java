/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.doc.FixFor;

class PostgresReadOnlyIncrementalSnapshotContextTest {

    private static final long XID_WRAP = 1L << 32;

    private PostgresOffsetContext.Loader offsetLoader;

    @BeforeEach
    void setUp() {
        final var config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .with(PostgresConnectorConfig.READ_ONLY_CONNECTION, true)
                .build());
        offsetLoader = new PostgresOffsetContext.Loader(config);
    }

    @Test
    @FixFor("debezium/dbz#2705")
    void expandsStreamingXidsToTheNearestEpoch() {
        final var snapshot = PgSnapshot.valueOf("4294967293:4294967300:4294967294,4294967295,4294967299");

        assertThat(snapshot.getFullTransactionId(0xffff_ffffL)).isEqualTo(XID_WRAP - 1);
        assertThat(snapshot.getFullTransactionId(3)).isEqualTo(XID_WRAP + 3);
        assertThat(snapshot.getFullTransactionId(4)).isEqualTo(XID_WRAP + 4);
        assertThat(snapshot.getXip()).contains(snapshot.getFullTransactionId(0xffff_fffeL));
        assertThat(snapshot.getXip()).contains(snapshot.getFullTransactionId(3));
    }

    @Test
    @FixFor("debezium/dbz#2705")
    void usesFullXidsForWindowAndVisibilityChecksAcrossWraparound() {
        final var context = new PostgresReadOnlyIncrementalSnapshotContext<>();
        context.setLowWatermark(PgSnapshot.valueOf("4294967294:4294967299:4294967294,4294967295"));
        context.setHighWatermark(PgSnapshot.valueOf("4294967299:4294967301:4294967300"));

        context.updateWindowState(offsetContext(0xffff_fffdL));
        assertThat(context.isWindowClosed()).isTrue();

        assertThat(context.isTransactionVisible(0xffff_ffffL)).isTrue();
        assertThat(context.isTransactionVisible(3L)).isTrue();
        assertThat(context.isTransactionVisible(4L)).isFalse();
        assertThat(context.maxInProgressTransactionCommitted(4L)).isFalse();
        assertThat(context.maxInProgressTransactionCommitted(5L)).isTrue();

        context.updateWindowState(offsetContext(0xffff_fffeL));
        assertThat(context.isWindowClosed()).isFalse();

        context.updateWindowState(offsetContext(5));
        assertThat(context.isWindowClosed()).isFalse();

        context.updateWindowState(offsetContext(6));
        assertThat(context.isWindowClosed()).isTrue();
    }

    private PostgresOffsetContext offsetContext(long transactionId) {
        final Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.TXID_KEY, transactionId);
        return offsetLoader.load(offset);
    }
}
