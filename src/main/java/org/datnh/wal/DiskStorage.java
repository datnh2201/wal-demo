package org.datnh.wal;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

// Simulates persistent storage (disk)
class DiskStorage {
    private final String dataFile;
    private final Map<String, Map<String, String>> persistentData = new HashMap<>();

    public DiskStorage(String dataFile) {
        this.dataFile = dataFile;
        loadFromDisk();
    }

    public synchronized void writePageToDisk(String table, String key, String value) {
        persistentData.computeIfAbsent(table, k -> new HashMap<>()).put(key, value);
        saveToDisk();
        System.out.println("  [DISK] Wrote " + table + "." + key + " = " + value + " to persistent storage");

        // Simulate disk write latency
        try {
            Thread.sleep(10); // 10ms per write to simulate disk latency
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public String readFromDisk(String table, String key) {
        return persistentData.getOrDefault(table, Collections.emptyMap()).get(key);
    }

    private void loadFromDisk() {
        try {
            if (Files.exists(Paths.get(dataFile))) {
                List<String> lines = Files.readAllLines(Paths.get(dataFile));
                for (String line : lines) {
                    String[] parts = line.split("\\|");
                    if (parts.length == 3) {
                        persistentData.computeIfAbsent(parts[0], k -> new HashMap<>()).put(parts[1], parts[2]);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error loading data from disk: " + e.getMessage());
        }
    }

    private void saveToDisk() {
        try {
            List<String> lines = new ArrayList<>();
            for (Map.Entry<String, Map<String, String>> tableEntry : persistentData.entrySet()) {
                for (Map.Entry<String, String> row : tableEntry.getValue().entrySet()) {
                    lines.add(tableEntry.getKey() + "|" + row.getKey() + "|" + row.getValue());
                }
            }
            Files.write(Paths.get(dataFile), lines);
        } catch (IOException e) {
            System.err.println("Error saving data to disk: " + e.getMessage());
        }
    }

    public void displayContents() {
        System.out.println("\n=== DISK STORAGE CONTENTS ===");
        if (persistentData.isEmpty()) {
            System.out.println("(No data on disk)");
        } else {
            for (Map.Entry<String, Map<String, String>> tableEntry : persistentData.entrySet()) {
                System.out.println("Table: " + tableEntry.getKey());
                for (Map.Entry<String, String> row : tableEntry.getValue().entrySet()) {
                    System.out.println("  " + row.getKey() + " = " + row.getValue());
                }
            }
        }
        System.out.println();
    }
}

