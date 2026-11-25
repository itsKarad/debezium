/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.debezium.connector.postgresql.connection.Lsn;

class PostgresReadOnlyIncrementalSnapshotContextTest {

    @Test
    void highWatermarkAdvancesOnlyAfterCommittedStreamPassesItsLsn() {
        final var context = new PostgresReadOnlyIncrementalSnapshotContext<>();
        context.setHighWatermark(new PgSnapshot(10L, 10L, Set.of()), Lsn.valueOf(100L));

        assertThat(context.canAdvanceWindow(Lsn.valueOf(99L))).isFalse();
        assertThat(context.canAdvanceWindow(Lsn.valueOf(100L))).isFalse();
        assertThat(context.canAdvanceWindow(Lsn.valueOf(101L))).isTrue();
    }

    @Test
    void highWatermarkDoesNotAdvanceWhileTransactionsWereInProgress() {
        final var context = new PostgresReadOnlyIncrementalSnapshotContext<>();
        context.setHighWatermark(new PgSnapshot(10L, 12L, Set.of(10L, 11L)), Lsn.valueOf(100L));

        assertThat(context.canAdvanceWindow(Lsn.valueOf(101L))).isFalse();
    }

    @Test
    void missingWalPositionDisablesTheNewAdvanceCheck() {
        final var context = new PostgresReadOnlyIncrementalSnapshotContext<>();
        context.setHighWatermark(new PgSnapshot(10L, 10L, Set.of()), null);

        assertThat(context.canAdvanceWindow(Lsn.valueOf(101L))).isFalse();
    }
}
