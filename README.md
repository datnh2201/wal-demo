# Understanding Write-Ahead Logging (WAL)

This project is a simple demonstration to help understand the concept of Write-Ahead Logging (WAL) and its role in ensuring transaction atomicity, durability and isolation in database systems.

## What is Write-Ahead Logging?

Write-Ahead Logging is a method to improve performance and provide atomicity and durability (two of the ACID properties) in database systems. 
The central idea is any changes to database system will only apply to the memory buffer first to improve performance 
and to ensure durability it will write WAL logs that represent change actions to disk, this writing action is sequential, making it much faster than random writes. 

If the database system crash when the changes are not yet applied to the disk, the system can recover by reading the log, applying the changes of committed transactions or undoing the changes of incomplete transactions.

## How it Works

1.  **Log First:** Before any changes are made to the actual data pages on disk, a log record represent the change action is written to a separate log file on stable storage.
2.  **Commit:** A transaction is considered "committed" once its corresponding log record has been successfully written to the log file. However, the changes made by the transaction may not be applied yet, only the memory buffer is updated to make changes visible for other transactions. 
3.  **Apply Changes:** The actual changes to the data files can be written later, at a more convenient time (e.g., during a checkpoint or when the memory buffer is full). The purpose of this is to improve performance by delaying random writes and allowing the system to batch updates.
4.  **Recovery:** In case of a system crash before the changes is written to the disk, the recovery process reads the log. It can redo the changes of committed transactions that weren't yet applied to the data files and undo the changes of incomplete transactions.

## Performance Improvement

The use of WAL significantly improves database performance by adopting the nature performance of disk I/O operations.

*   **Sequential vs. Random Writes:** Writing to the log is a sequential append operation, which is much faster than the random writes required to update various data pages scattered across the disk.
*   **Reduced I/O:** Instead of flushing every modified data page to disk for each transaction, only the log record needs to be written. The actual data page writes can be batched and performed more efficiently in the background, reducing the overall I/O load and improving transaction throughput.

## How to Run

This project uses Gradle. You can build and run it using the following commands:

```bash
# Build the project
./gradlew build

# Run the main application
./gradlew run
```

Disclaimer: Please note that this implementation is for demonstration purposes only to illustrate how WAL works. It is not optimized for performance, may not handle all possible bugs or edge cases, and is not intended for production use.