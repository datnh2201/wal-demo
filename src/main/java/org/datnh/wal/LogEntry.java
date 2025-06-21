package org.datnh.wal;

// Represents a single log entry in the WAL
class LogEntry {
    private final long lsn; // Log Sequence Number
    private final long transactionId;
    private final String operation; // INSERT, UPDATE, DELETE
    private final String table;
    private final String key;
    private final String oldValue;
    private final String newValue;
    private final long timestamp;

    public LogEntry(long lsn, long transactionId, String operation, String table,
                    String key, String oldValue, String newValue) {
        this.lsn = lsn;
        this.transactionId = transactionId;
        this.operation = operation;
        this.table = table;
        this.key = key;
        this.oldValue = oldValue;
        this.newValue = newValue;
        this.timestamp = System.currentTimeMillis();
    }

    // Getters
    public long getLsn() {
        return lsn;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public String getOperation() {
        return operation;
    }

    public String getTable() {
        return table;
    }

    public String getKey() {
        return key;
    }

    public String getOldValue() {
        return oldValue;
    }

    public String getNewValue() {
        return newValue;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.format("LSN:%d|TXN:%d|%s|%s|%s|%s->%s|%d",
                lsn, transactionId, operation, table, key, oldValue, newValue, timestamp);
    }
}