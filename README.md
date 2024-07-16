# Columnar Database Implementation for Minibase

This project focuses on enhancing the Minibase relational database by implementing a columnar storage model tailored for analytical workloads. The primary objective is to improve data retrieval and processing efficiencies using advanced indexing and storage mechanisms.

## Project Overview

Minibase is a streamlined relational database system built for educational usage that incorporates essential components such as disk management, indexing, query execution, and buffer management. This project enhances Minibase by integrating a columnar database structure, which is optimized for analytical queries that typically involve operations on individual data columns.

### Key Features

- **Columnar Storage:** Data is partitioned into columns, and each data page contains data from a single column, significantly improving retrieval and processing speeds of analytical workloads.
- **Bitmap Indexing:** Enhanced performance for columnar databases by using Bitmap indices instead of traditional B+ trees, providing superior performance for specific column data access.
- **Efficient Data Loading and Indexing:** Bulk data loading, index creation, and querying mechanisms have been rigorously tested to ensure optimal performance.

## Implementation Details

### Main Modules

1. **TID:** Acts as an ID for tuples in the columnar store, managing RIDs for each column.
2. **ColumnarFile:** Manages the storage and retrieval of columnar data.
3. **TupleScan:** Provides mechanisms for scanning and retrieving tuples.
4. **Bitmap Index:** Implementation of Bitmap indices for efficient querying.
5. **ColumnarFileScan:** Allows scanning of columnar files based on query constraints.
6. **Indexing:** Supports both B+ tree and Bitmap indexing.

### System Requirements

**Hardware:**
- Processor: Intel i5 or above / Ryzen 5 or above / M2 or above
- RAM: 8GB DDR4 or more
- Disk Space: Amount of data to be stored + 500 MB of free disk space

**Software:**
- Operating System: Windows 11 / OSX Sonoma or above / Ubuntu 22.04 or above
- JDK: Java 12 or later / OpenJDK 12 or later
- JRE: Version 11 or later
- IDE: IntelliJ recommended, also compatible with VSCode

## User Interfaces
Once the files are compiled using
```sh
javac -d out/ -cp src/ src/tests/filename.java
```
they can be run using the following commands where the required parameters are passed to the respective compiled files.
### Batch Insert

Bulk insert data into the columnar database from a text file.

```sh
java -cp out/ tests.BatchInsert DATAFILENAME COLUMNDBNAME COLUMNARFILENAME NUMCOLUMNS
```

### Indexing

Create indices on specified columns.

```sh
java -cp out/ tests.Index COLUMNDBNAME COLUMNARFILENAME COLUMNNUMBER INDEXTYPE
```

### Query

Execute queries on the columnar database.

```sh
java -cp out/ tests.Query COLUMNDBNAME COLUMNARFILENAME COLUMNSTOBERETURNED VALUECONSTRAINT COLUMNSTOBESCANNED ACCESSTYPE [TARGETCOLUMNNAMES] NUMBUF
```

### Delete Query

Delete data from the columnar database.

```sh
java -cp out/ tests.Delete [COLUMNDB NAME] [CF NAME] [PROJECTION] [DELETE
CONDITION] [COLS TO BE SCANNED] [SCAN TYPES] [SCAN CONSTRAINTS TO BE USED
ON INDICES] [TARGET COLS] [BUFFER SIZE] [PURGE/NOT]
```


## Conclusion

This project has significantly improved the Minibase system's functionality, making it efficient for complex data handling in analytical environments. The enhancements include advanced indexing, efficient data sorting, and improved query response times, positioning Minibase as a robust solution for analytical database management.
