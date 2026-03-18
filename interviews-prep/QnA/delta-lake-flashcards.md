# 🧠 Delta Lake Flashcards

---

## 1. What is Delta Lake?
**Q:** What is Delta Lake?  
**A:** Delta Lake is a storage layer built on top of data lakes that adds ACID transactions, schema enforcement, time travel, and efficient data management to Parquet-based data.

---

## 2. What problem does Delta Lake solve?
**Q:** What problems does Delta Lake solve in data lakes?  
**A:** It solves lack of ACID transactions, schema inconsistency, poor performance, and difficulty in updates/deletes.

---

## 3. What are ACID transactions in Delta Lake?
**Q:** What does ACID mean in Delta Lake?  
**A:** ACID stands for Atomicity, Consistency, Isolation, and Durability, ensuring reliable and consistent data operations.

---

## 4. How does Delta Lake ensure atomicity?
**Q:** How does Delta Lake ensure atomicity?  
**A:** By committing changes only when the entire transaction is successful, avoiding partial writes.

---

## 5. What is the Delta transaction log?
**Q:** What is the Delta transaction log?  
**A:** It is a log stored in the `_delta_log` folder that tracks all changes, versions, and metadata of a Delta table.

---

## 6. What is stored in the `_delta_log` folder?
**Q:** What does the `_delta_log` contain?  
**A:** JSON files and checkpoints containing transaction history, schema, and metadata.

---

## 7. What is time travel in Delta Lake?
**Q:** What is time travel in Delta Lake?  
**A:** The ability to query previous versions of a table using version number or timestamp.

---

## 8. How do you query a previous version?
**Q:** How can you query an older version of a Delta table?  
**A:** Using `VERSION AS OF` or `TIMESTAMP AS OF` in SQL queries.

---

## 9. What is schema enforcement?
**Q:** What is schema enforcement in Delta Lake?  
**A:** It ensures that data written to a table matches the defined schema, preventing invalid writes.

---

## 10. What is schema evolution?
**Q:** What is schema evolution?  
**A:** It allows changes to the schema, such as adding new columns, while maintaining compatibility.

---

## 11. What is MERGE INTO?
**Q:** What is MERGE INTO in Delta Lake?  
**A:** It is an operation used for upserts (update + insert) into Delta tables.

---

## 12. Why is MERGE important?
**Q:** Why is MERGE INTO important in data engineering?  
**A:** It allows efficient updates and inserts without rewriting entire datasets.

---

## 13. What is data versioning?
**Q:** What is data versioning in Delta Lake?  
**A:** Every change creates a new version of the table, enabling history tracking and rollback.

---

## 14. What file format does Delta Lake use?
**Q:** What file format does Delta Lake use?  
**A:** Delta Lake uses Parquet files for storage along with a transaction log.

---

## 15. What is OPTIMIZE in Delta Lake?
**Q:** What does OPTIMIZE do?  
**A:** It compacts small files into larger ones to improve query performance.

---

## 16. What is Z-Ordering?
**Q:** What is Z-Ordering in Delta Lake?  
**A:** A technique that colocates related data in files to improve query performance.

---

## 17. What is data skipping?
**Q:** What is data skipping?  
**A:** Delta Lake uses metadata to skip irrelevant files during queries, reducing data scanned.

---

## 18. What is VACUUM?
**Q:** What does VACUUM do in Delta Lake?  
**A:** It removes old files that are no longer needed, freeing storage space.

---

## 19. What is the difference between Delta and Parquet?
**Q:** How is Delta Lake different from Parquet?  
**A:** Delta adds transaction logs, ACID guarantees, schema enforcement, and versioning on top of Parquet.

---

## 20. Why use Delta Lake with Spark?
**Q:** Why is Delta Lake used with Spark?  
**A:** It improves reliability, performance, and flexibility when working with large-scale data in Spark environments.