/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.connector.postgresql.connection.Lsn;
import io.debezium.pipeline.source.snapshot.incremental.AbstractIncrementalSnapshotContext;
import io.debezium.pipeline.spi.OffsetContext;

/**
 * A class describing PostgreSQL read only incremental snapshot context state.
 *
 * @author Mario Fiore Vitale
 */
public class PostgresReadOnlyIncrementalSnapshotContext<T> extends AbstractIncrementalSnapshotContext<T> {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresReadOnlyIncrementalSnapshotContext.class);

    private PgSnapshot lowWatermark;
    private Watermark highWatermark;
    private Watermark previousHighWatermark;
    private PgSnapshot previousLowWatermark;

    private record Watermark(PgSnapshot snapshot, Lsn lsn) {
    }

    public PostgresReadOnlyIncrementalSnapshotContext() {
        this(false);
    }

    public PostgresReadOnlyIncrementalSnapshotContext(boolean useCatalogBeforeSchema) {
        super(useCatalogBeforeSchema);
    }

    public static <U> PostgresReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets) {
        return load(offsets, false);
    }

    public static <U> PostgresReadOnlyIncrementalSnapshotContext<U> load(Map<String, ?> offsets, boolean useCatalogBeforeSchema) {
        PostgresReadOnlyIncrementalSnapshotContext<U> context = new PostgresReadOnlyIncrementalSnapshotContext<>(useCatalogBeforeSchema);
        init(context, offsets);
        return context;
    }

    public PgSnapshot getLowWatermark() {
        return lowWatermark;
    }

    public void setLowWatermark(PgSnapshot lowWatermark) {
        LOGGER.trace("Setting low watermark to {}", lowWatermark);
        this.lowWatermark = lowWatermark;
    }

    public PgSnapshot getHighWatermark() {
        return highWatermark == null ? null : highWatermark.snapshot();
    }

    public void setHighWatermark(PgSnapshot highWatermark, Lsn highWatermarkLsn) {
        LOGGER.trace("Setting high watermark to {} at LSN {}", highWatermark, highWatermarkLsn);
        this.highWatermark = new Watermark(highWatermark, highWatermarkLsn);
    }

    public void updateWindowState(OffsetContext offsetContext) {

        Long eventTxId = offsetContext.getSourceInfo().getInt64(SourceInfo.TXID_KEY);
        LOGGER.trace("Received event with TxId {}", eventTxId);
        LOGGER.trace("Updating window. Window opened: {}, low watermark {}, high watermark {}", windowOpened, lowWatermark, getHighWatermark());

        if (!windowOpened && lowWatermark != null) {
            boolean pastLowWatermark = eventTxId >= lowWatermark.getXMin();
            if (pastLowWatermark) {
                LOGGER.debug("Current event txId {}, low watermark {}", eventTxId, lowWatermark);
                windowOpened = true;
            }
        }
        if (windowOpened && highWatermark != null) {
            boolean pastHighWatermark = eventTxId > Math.max(highWatermark.snapshot().getXMax(), lowWatermark.getXMax());
            if (pastHighWatermark) {
                LOGGER.debug("Current event txId {}, high watermark {}", eventTxId, highWatermark.snapshot());
                closeWindow();
            }
        }
    }

    public boolean isWindowClosed() {
        return !windowOpened;
    }

    public void closeWindow() {
        LOGGER.trace("Window closed. Low and High watermark cleaned");
        windowOpened = false;
        previousHighWatermark = highWatermark;
        previousLowWatermark = lowWatermark;
        highWatermark = null;
        lowWatermark = null;
    }

    public boolean isTransactionVisible(Long eventTxId) {

        if (highWatermark == null) {
            return false;
        }
        return eventTxId.compareTo(highWatermark.snapshot().getXMin()) <= 0;
    }

    public boolean canAdvanceWindow(Lsn lastCommittedStreamLsn) {
        return highWatermark != null
                && highWatermark.snapshot().getXip().isEmpty()
                && highWatermark.lsn() != null
                && lastCommittedStreamLsn != null
                && lastCommittedStreamLsn.compareTo(highWatermark.lsn()) > 0;
    }

    public boolean watermarksChanged() {
        LOGGER.trace("previousLowWatermark {}, lowWatermark {}, previousHighWatermark {}, highWatermark {}",
                previousLowWatermark, lowWatermark, previousHighWatermark, highWatermark);
        return !Objects.equals(previousLowWatermark, lowWatermark)
                || !Objects.equals(previousHighWatermark, highWatermark);
    }
}
