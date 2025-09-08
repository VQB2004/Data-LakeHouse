## 📖 Introduction
**MiniLakehouse** is a lightweight reference implementation of a modern **Data Lakehouse** architecture, deployed with **Docker**.  
It demonstrates an end-to-end flow for ingesting data from an operational database, storing it in object storage as columnar files, managing table metadata with a table-format + versioned catalog, querying with a distributed SQL engine, and visualizing results.
## 🏗️ Architecture Overview
#### 1. Data Source — **MySQL**
#### 2. Object Storage — **Minio**
- S3-compatible object store acting as the physical data lake.
- Stores Parquet files and Iceberg metadata files (manifests, metadata.json).
#### 3. Table Format + Metadata — **Apache Iceberg + Project Nessie**
- **Apache Iceberg**: provides a table abstraction over files in object storage.
  - Manages snapshots, partitioning, schema evolution, and ACID-like atomic operations on tables.
  - Iceberg stores metadata files that reference Parquet file locations.
- **Project Nessie**: a **versioned catalog** (Git-like) for Iceberg metadata.
#### 4. Query Engine — **Trino**
- Distributed SQL engine that executes queries directly on Iceberg tables (via the Nessie-backed catalog).
- Reads Parquet files from MinIO, uses Iceberg metadata (via Nessie) to resolve snapshots/partitions.
#### 5. Visualization — **Metabase**
- BI layer that connects to Trino to build charts, dashboards and ad-hoc queries.
- Provides business-facing dashboards on top of the lakehouse.
#### 6. Containerization — **Docker**
![Lakehouse Architecture](images/lakehouse_architecture.png)
