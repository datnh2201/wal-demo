package org.datnh.wal;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class represents a buffer pool that holds data in memory.
 * Any read or write operations will need to go through this buffer pool to make sure it populate on latest data.
 */
class BufferPool {
    // Maps table names to their key-value pairs
    private final Map<String, Map<String, String>> tables = new ConcurrentHashMap<>();

    // changed data not yet written to disk, in the real world, it will be written to disk later when the buffer is full or at checkpoint
    private final Set<String> dirtyPages = ConcurrentHashMap.newKeySet();

    public void put(String table, String key, String value) {
        tables.computeIfAbsent(table, k -> new ConcurrentHashMap<>()).put(key, value);
        dirtyPages.add(table + ":" + key);
        System.out.println("  [BUFFER] Updated " + table + "." + key + " = " + value + " (in memory)");
    }

    public String get(String table, String key) {
        return tables.getOrDefault(table, Collections.emptyMap()).get(key);
    }

    public Set<String> getDirtyPages() {
        return new HashSet<>(dirtyPages);
    }

    public void markClean(String page) {
        dirtyPages.remove(page);
    }

    public void displayContents() {
        System.out.println("\n=== BUFFER POOL CONTENTS ===");
        for (Map.Entry<String, Map<String, String>> tableEntry : tables.entrySet()) {
            System.out.println("Table: " + tableEntry.getKey());
            for (Map.Entry<String, String> row : tableEntry.getValue().entrySet()) {
                String pageKey = tableEntry.getKey() + ":" + row.getKey();
                String status = dirtyPages.contains(pageKey) ? " (DIRTY)" : " (CLEAN)";
                System.out.println("  " + row.getKey() + " = " + row.getValue() + status);
            }
        }
        System.out.println();
    }
}