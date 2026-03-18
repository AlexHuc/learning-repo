# 🧠 Common Questions - Spark / Delta / Databricks Flashcards

---

## 1. What is Apache Spark and how does it work?
**Q:** What is Apache Spark and how does it work?  
**A:** Apache Spark is a distributed data processing engine that processes large datasets in parallel across a cluster. It uses a driver to coordinate tasks and executors to process data partitions.

---

## 2. What is the difference between RDD, DataFrame, and Dataset?
**Q:** What is the difference between RDD, DataFrame, and Dataset?  
**A:** RDD is a low-level distributed collection of objects, DataFrame is a structured dataset with schema and optimizations, and Dataset is a strongly-typed version of DataFrame (mainly in Scala/Java).

---

## 3. What is lazy evaluation?
**Q:** What is lazy evaluation?  
**A:** Lazy evaluation means transformations are not executed until an action is triggered, allowing Spark to optimize execution plans.

---

## 4. What is a shuffle and why is it expensive?
**Q:** What is a shuffle and why is it expensive?  
**A:** A shuffle redistributes data across partitions, typically during joins or aggregations. It is expensive due to network transfer, disk I/O, and data movement.

---

## 5. What is a broadcast join and when do you use it?
**Q:** What is a broadcast join and when do you use it?  
**A:** A broadcast join sends a small dataset to all executors to avoid shuffle. It is used when one table is small enough to fit in memory.

---

## 6. How do you optimize a slow Spark job?
**Q:** How do you optimize a slow Spark job?  
**A:** By analyzing Spark UI, reducing shuffles, using broadcast joins, optimizing partitions, caching data, and improving query logic.

---

## 7. How do you handle skewed data?
**Q:** How do you handle skewed data?  
**A:** By using broadcast joins, salting, repartitioning, adaptive query execution, or pre-aggregation.

---

## 8. What is Delta Lake and why use it over Parquet?
**Q:** What is Delta Lake and why use it over Parquet?  
**A:** Delta Lake adds ACID transactions, schema enforcement, versioning, and efficient updates on top of Parquet files.

---

## 9. What are ACID transactions in Delta Lake?
**Q:** What are ACID transactions in Delta Lake?  
**A:** ACID ensures reliable operations: Atomicity, Consistency, Isolation, and Durability.

---

## 10. What is Time Travel?
**Q:** What is Time Travel in Delta Lake?  
**A:** It allows querying previous versions of a table using version number or timestamp.

---

## 11. What is Schema Enforcement vs Schema Evolution?
**Q:** What is Schema Enforcement vs Schema Evolution?  
**A:** Schema enforcement prevents invalid schema writes, while schema evolution allows controlled schema changes like adding new columns.

---

## 12. How do you implement an upsert (SCD Type 1)?
**Q:** How do you implement an upsert (SCD Type 1)?  
**A:** By using MERGE INTO to update existing records and insert new ones without preserving history.

---

## 13. How do you implement SCD Type 2 in Delta Lake?
**Q:** How do you implement SCD Type 2 in Delta Lake?  
**A:** By creating new records for changes, updating old records with end dates, and marking current records using flags.

---

## 14. How do you handle duplicates?
**Q:** How do you handle duplicates in data pipelines?  
**A:** By using deduplication logic such as DISTINCT, dropDuplicates(), or window functions.

---

## 15. What is the Delta Log (_delta_log)?
**Q:** What is the Delta Log (_delta_log)?  
**A:** It is a transaction log that stores metadata, schema, and version history of a Delta table.

---

## 16. What does VACUUM do and what are the risks?
**Q:** What does VACUUM do and what are the risks?  
**A:** VACUUM removes old data files to free space, but can break time travel if retention is too short.

---

## 17. What happens if you overwrite a Delta table?
**Q:** What happens when you overwrite a Delta table?  
**A:** A new version is created and previous data is logically replaced, but still accessible via time travel (until vacuumed).

---

## 18. What is a cluster?
**Q:** What is a cluster in Spark/Databricks?  
**A:** A cluster is a group of machines (nodes) that work together to process data in parallel.

---