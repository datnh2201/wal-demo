package org.datnh.wal;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class represents a transaction in the database system, manage uncommitted changes and ensure durability and isolation.
 * It provides methods to perform read, update, insert, commit and rollback operations
 */
class Transaction {
    private static final AtomicLong transactionIdCounter = new AtomicLong(0);
    private final long transactionId;
    private final WriteAheadLog wal;
    private final BufferPool bufferPool;

    // Transaction log represents all changes made during this transaction
    private final List<LogEntry> transactionLog = new ArrayList<>();

    // Transaction-local buffer to hold uncommitted changes
    private final Map<String, Map<String, String>> transactionBuffer = new HashMap<>();
    private boolean committed = false;

    public Transaction(WriteAheadLog wal, BufferPool bufferPool) {
        this.transactionId = transactionIdCounter.incrementAndGet();
        this.wal = wal;
        this.bufferPool = bufferPool;
        System.out.println("\n[TXN-" + transactionId + "] Transaction started");
    }

    /**
     * Update the value associated with the key in the specified table.
     * This method buffers the change but does not write to WAL yet.
     *
     * @param table      name of the table to update
     * @param key        the key to update
     * @param oldValue   the expected old value (for validation)
     * @param newValue   the new value to set
     */
    public void update(String table, String key, String oldValue, String newValue) {
        if (committed) {
            throw new IllegalStateException("Transaction already committed");
        }

        // Get the current value as seen by this transaction (could be from transaction buffer or buffer pool)
        String actualOldValue = read(table, key);
        if (actualOldValue == null) {
            actualOldValue = oldValue; // Use provided old value if record doesn't exist
        }

        // Create log entry but don't write to WAL yet - just buffer it
        long lsn = wal.getNextLSN();
        LogEntry entry = new LogEntry(lsn, transactionId, "UPDATE", table, key, actualOldValue, newValue);
        transactionLog.add(entry);

        // Update transaction-local buffer so this transaction can see its own changes
        transactionBuffer.computeIfAbsent(table, k -> new HashMap<>()).put(key, newValue);

        System.out.println("[TXN-" + transactionId + "] Buffered UPDATE " + table + "." + key + " = " + newValue + " (visible to this transaction only)");
    }

    /**
     * Insert a new key-value pair into the specified table.
     * This method buffers the change but does not write to WAL yet.
     *
     * @param table name of the table to insert into
     * @param key   the key to insert
     * @param value the value to insert
     */
    public void insert(String table, String key, String value) {
        if (committed) {
            throw new IllegalStateException("Transaction already committed");
        }

        // Create log entry but don't write to WAL yet - just buffer it
        long lsn = wal.getNextLSN();
        LogEntry entry = new LogEntry(lsn, transactionId, "INSERT", table, key, null, value);
        transactionLog.add(entry);

        // Update transaction-local buffer so this transaction can see its own changes
        transactionBuffer.computeIfAbsent(table, k -> new HashMap<>()).put(key, value);

        System.out.println("[TXN-" + transactionId + "] Buffered INSERT " + table + "." + key + " = " + value + " (visible to this transaction only)");
    }

    /**
     * read the value associated with the kye in given table.
     * First this method read value from transaction local buffer to make sure have lastest value.
     * If value is not found the transaction local buffer, it will read from buffer pool.
     * @param table name of the table to read from
     * @param key the key to read
     * @return the value associated with the given key in the table, or null if not found.
     */
    public String read(String table, String key) {
        if (committed) {
            throw new IllegalStateException("Transaction already committed");
        }

        // First check transaction-local buffer (uncommitted changes)
        String value = transactionBuffer.getOrDefault(table, Collections.emptyMap()).get(key);
        if (value != null) {
            System.out.println("[TXN-" + transactionId + "] READ " + table + "." + key + " = " + value + " (from transaction buffer)");
            return value;
        }

        // Fall back to buffer pool (committed data)
        value = bufferPool.get(table, key);
        if (value != null) {
            System.out.println("[TXN-" + transactionId + "] READ " + table + "." + key + " = " + value + " (from buffer pool)");
        } else {
            System.out.println("[TXN-" + transactionId + "] READ " + table + "." + key + " = NULL (not found)");
        }
        return value;
    }

    /**
     * Commit the transaction to make change durable.
     * <br/> This method will write all buffered WAL log entries associated with this transaction to the Write-Ahead Log (WAL)
     * and apply changes to the buffer pool to make it visible to other transactions
     */
    public void commit() {
        if (committed) {
            throw new IllegalStateException("Transaction already committed");
        }

        System.out.println("[TXN-" + transactionId + "] COMMIT - Writing " + transactionLog.size() + " log entries to WAL...");

        // Now write all buffered log entries to WAL atomically
        for (LogEntry entry : transactionLog) {
            wal.writeLogEntryToDisk(entry);
        }

        // Force WAL to disk (synchronous commit) to ensure durability
        wal.forceWALToDisk();

        // Only after WAL is safely on disk, apply changes to buffer pool to make it visible to other transactions.
        for (LogEntry entry : transactionLog) {
            wal.applyToBufferPool(entry);
        }

        committed = true;
        System.out.println("[TXN-" + transactionId + "] COMMIT successful - All changes are durable and visible to other transactions");
    }

    public void rollback() {
        if (committed) {
            throw new IllegalStateException("Cannot rollback committed transaction");
        }

        System.out.println("[TXN-" + transactionId + "] ROLLBACK - Discarding " + transactionLog.size() + " buffered changes");
        transactionLog.clear();
        transactionBuffer.clear();
        committed = true; // Mark as finished
    }
}

