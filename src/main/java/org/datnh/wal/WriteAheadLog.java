package org.datnh.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

// Main WAL implementation
class WriteAheadLog {
    private final AtomicLong lsnCounter = new AtomicLong(0);
    private final String walFile;
    private final BufferPool bufferPool;
    private final DiskStorage diskStorage;
    private final List<LogEntry> logEntries = new ArrayList<>();
//    private final ScheduledExecutorService checkpointExecutor;

    public WriteAheadLog(String walFile, BufferPool bufferPool, DiskStorage diskStorage) {
        this.walFile = walFile;
        this.bufferPool = bufferPool;
        this.diskStorage = diskStorage;
//        this.checkpointExecutor = Executors.newScheduledThreadPool(1);
//
//        // Schedule periodic checkpoints every 10 seconds
//        checkpointExecutor.scheduleAtFixedRate(this::checkpoint, 10, 10, TimeUnit.SECONDS);
    }

    public synchronized long getNextLSN() {
        return lsnCounter.incrementAndGet();
    }

    public synchronized void writeLogEntryToDisk(LogEntry entry) {
        // Add to our in-memory log
        logEntries.add(entry);

        // Write to WAL file (but don't force to disk yet)
        try {
            String logLine = entry.toString() + "\n";
            Files.write(Paths.get(walFile), logLine.getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            System.out.println("  [WAL] Written to log buffer: " + entry);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write WAL entry", e);
        }
    }

    public synchronized void forceWALToDisk() {
        // In a real database, this would call fsync() to force OS buffers to disk
        // For simulation, we just print that we're forcing the flush
        System.out.println("  [WAL] FORCE SYNC - All log entries are now durable on disk");

        // Simulate the time it takes to force data to disk
        try {
            Thread.sleep(5); // 5ms to simulate fsync() call
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public void applyToBufferPool(LogEntry entry) {
        if (!"DELETE".equals(entry.getOperation())) {
            bufferPool.put(entry.getTable(), entry.getKey(), entry.getNewValue());
        }
        System.out.println("  [BUFFER] Applied: " + entry.getOperation() + " " +
                entry.getTable() + "." + entry.getKey() + " = " + entry.getNewValue());
    }

    public void checkpoint() {
        System.out.println("\n>>> CHECKPOINT STARTED <<<");
        Set<String> dirtyPages = bufferPool.getDirtyPages();

        if (dirtyPages.isEmpty()) {
            System.out.println("No dirty pages to checkpoint.");
            return;
        }

        for (String page : dirtyPages) {
            String[] parts = page.split(":");
            String table = parts[0];
            String key = parts[1];
            String value = bufferPool.get(table, key);

            if (value != null) {
                diskStorage.writePageToDisk(table, key, value);
                bufferPool.markClean(page);
            }
        }

        System.out.println(">>> CHECKPOINT COMPLETED <<<\n");
    }

    public void recover() {
        System.out.println("\n>>> RECOVERY STARTED <<<");

        try {
            if (!Files.exists(Paths.get(walFile))) {
                System.out.println("No WAL file found. Nothing to recover.");
                return;
            }

            List<String> logLines = Files.readAllLines(Paths.get(walFile));
            System.out.println("Replaying " + logLines.size() + " log entries...");

            for (String line : logLines) {
                String[] parts = line.split("\\|");
                if (parts.length >= 7) {
                    String operation = parts[2];
                    String table = parts[3];
                    String key = parts[4];
                    String newValue = parts[5].split("->")[1];

                    // Replay the operation
                    if (!"DELETE".equals(operation)) {
                        bufferPool.put(table, key, newValue);
                    }
                    System.out.println("  [RECOVERY] Replayed: " + operation + " " + table + "." + key);
                }
            }

            System.out.println(">>> RECOVERY COMPLETED <<<\n");
        } catch (IOException e) {
            System.err.println("Error during recovery: " + e.getMessage());
        }
    }

    public void shutdown() {
        checkpoint(); // Final checkpoint
//        checkpointExecutor.shutdown();
    }
}
