package org.datnh.wal;

import java.util.Objects;

// Main simulation class
public class WALSimulation {
    public static void main(String[] args) {
        System.out.println("=== WAL (Write-Ahead Log) Simulation ===\n");

        // Initialize components
        BufferPool bufferPool = new BufferPool();
        DiskStorage diskStorage = new DiskStorage("data.txt");
        WriteAheadLog wal = new WriteAheadLog("wal.log", bufferPool, diskStorage);

        // Show initial state
        System.out.println("Initial state:");
        diskStorage.displayContents();

        // Simulate recovery on startup
        wal.recover();

        try {
            // Simulate some transactions
//            simulateTransactions(wal, bufferPool, diskStorage);
            simulateTransactionIsolation(wal, bufferPool, diskStorage);

            // Wait a bit to see checkpoint in action
            Thread.sleep(12000);

            // Show final state
            System.out.println("Final state:");
            bufferPool.displayContents();
            diskStorage.displayContents();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            wal.shutdown();
        }
    }

    private static void simulateTransactionIsolation(WriteAheadLog wal, BufferPool bufferPool, DiskStorage diskStorage) {
        bufferPool.put("accounts", "alice", "1000");
        bufferPool.put("accounts", "bob", "500");

        Transaction txn1 = new Transaction(wal, bufferPool);
        Transaction txn2 = new Transaction(wal, bufferPool);

        String aliceBalanceFromTx1 = txn1.read("accounts", "alice");
        String aliceBalanceFromTx2 = txn2.read("accounts", "alice");

        System.out.println("Read Alice's balance in TXN-1: " + aliceBalanceFromTx1);
        System.out.println("Read Alice's balance in TXN-2: " + aliceBalanceFromTx2);
        assert Objects.equals(aliceBalanceFromTx1, aliceBalanceFromTx2);

        // try to simulate isolation change between 2 transactions
        txn1.update("accounts", "alice", aliceBalanceFromTx1, "1200");
        aliceBalanceFromTx1 = txn1.read("accounts", "alice");
        aliceBalanceFromTx2 = txn2.read("accounts", "alice");
        assert !Objects.equals(aliceBalanceFromTx1, aliceBalanceFromTx2);

        // should see the updated value in txn1: 1200
        System.out.println("Read Alice's balance in TXN-1: " + aliceBalanceFromTx1);

        // should still see the old value in txn2, because transaction 1 is not committed yet.
        System.out.println("Read Alice's balance in TXN-2: " + aliceBalanceFromTx2);

        txn1.commit();
        txn2.update("accounts", "bob", "500", "1500");
        txn2.commit();
    }

    private static void simulateTransactions(WriteAheadLog wal, BufferPool bufferPool, DiskStorage diskStorage) {
        // Transaction 1: Bank account operations that read their own changes
        Transaction txn1 = new Transaction(wal, bufferPool);

        // Initial setup - put some data in buffer pool
        bufferPool.put("accounts", "alice", "1000");
        bufferPool.put("accounts", "bob", "500");

        System.out.println("\n--- Demonstrating transaction can see its own uncommitted changes ---");

        // Update Alice's account
        txn1.update("accounts", "alice", "1000", "1200");

        // Now read Alice's balance - should see the updated value within the same transaction
        String aliceBalance = txn1.read("accounts", "alice");
        System.out.println("Alice's balance as seen by TXN-1: " + aliceBalance);

        // Update Alice again based on previous change
        txn1.update("accounts", "alice", aliceBalance, "1300");

        // Read again to confirm we see the latest change
        aliceBalance = txn1.read("accounts", "alice");
        System.out.println("Alice's updated balance as seen by TXN-1: " + aliceBalance);

        txn1.update("accounts", "bob", "500", "300");
        txn1.commit();

        System.out.println("\n--- Starting another transaction to show isolation ---");

        // Transaction 2: User profile updates
        Transaction txn2 = new Transaction(wal, bufferPool);

        // This transaction should see Alice's committed balance (1300)
        String aliceBalanceFromTxn2 = txn2.read("accounts", "alice");
        System.out.println("Alice's balance as seen by TXN-2: " + aliceBalanceFromTxn2);

        txn2.insert("users", "user123", "John Doe");
        txn2.update("users", "user456", "Jane Smith", "Jane Johnson");

        // Read the inserted user within the same transaction
        String user123 = txn2.read("users", "user123");
        System.out.println("User123 as seen by TXN-2: " + user123);

        txn2.commit();

        // Transaction 3: Demonstrate rollback with reads
        Transaction txn3 = new Transaction(wal, bufferPool);

        String originalBalance = txn3.read("accounts", "alice");
        txn3.update("accounts", "alice", originalBalance, "1500");

        // Verify we can see our own uncommitted change
        String tempBalance = txn3.read("accounts", "alice");
        System.out.println("Alice's temporary balance in TXN-3: " + tempBalance);

        txn3.insert("users", "user789", "Test User");
        String testUser = txn3.read("users", "user789");
        System.out.println("Test user as seen by TXN-3: " + testUser);

        System.out.println("Simulating transaction failure...");
        txn3.rollback(); // This transaction is rolled back

        // Start a new transaction to verify rollback worked
        Transaction txn4 = new Transaction(wal, bufferPool);
        String aliceAfterRollback = txn4.read("accounts", "alice");
        String testUserAfterRollback = txn4.read("users", "user789");

        System.out.println("Alice's balance after TXN-3 rollback: " + aliceAfterRollback + " (should be 1300)");
        System.out.println("Test user after TXN-3 rollback: " + testUserAfterRollback + " (should be NULL)");

        txn4.commit(); // Just close the read transaction

        // Show current state
        bufferPool.displayContents();
        diskStorage.displayContents();

        System.out.println("Notice: Changes are in buffer pool but not yet on disk!");
        System.out.println("Waiting for checkpoint to write to disk...");
    }
}