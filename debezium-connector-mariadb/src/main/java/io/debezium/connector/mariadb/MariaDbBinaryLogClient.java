/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.mariadb;

import java.io.IOException;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.GtidSet;

final class MariaDbBinaryLogClient extends BinaryLogClient {

    MariaDbBinaryLogClient(String hostname, int port, String username, String password) {
        super(hostname, port, username, password);
    }

    @Override
    protected void requestBinaryLogStreamMaria(long serverId) throws IOException {
        if (!isUseBinlogFilenamePositionInGtidMode()) {
            super.requestBinaryLogStreamMaria(serverId);
            return;
        }

        // The base client's MariaDB path does not consult the file-position flag. Suppress the
        // GTID set only for the dump request so the server starts at the recorded replay position.
        synchronized (gtidSetAccessLock) {
            final GtidSet savedGtidSet = gtidSet;
            try {
                gtidSet = null;
                super.requestBinaryLogStreamMaria(serverId);
            }
            finally {
                gtidSet = savedGtidSet;
            }
        }
    }
}
