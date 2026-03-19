# 🧠 Technical Data Engineering Flashcards

---

## 1. How does Spark execute a job internally?
**Q:** What happens when you run a Spark job?  
**A:** The driver builds a DAG of transformations, splits it into stages based on shuffle boundaries, and assigns tasks to executors for parallel execution.

---

## 2. What causes a shuffle in Spark?
**Q:** When does a shuffle occur?  
**A:** A shuffle happens when data needs to be redistributed across partitions, typically during joins, groupBy, or aggregations.

---

## 3. Why is data skew a problem?
**Q:** Why does data skew impact performance?  
**A:** Because uneven data distribution causes some tasks to take much longer, leading to bottlenecks and underutilized resources.

---

## 4. How would you debug a slow Spark job?
**Q:** How do you investigate a slow Spark job?  
**A:** By analyzing the Spark UI, checking stages, identifying skewed tasks, examining shuffles, and reviewing execution plans.

---

## 5. What is predicate pushdown?
**Q:** What is predicate pushdown?  
**A:** It is an optimization where filters are applied as early as possible, reducing the amount of data read and processed.

---

## 6. What is partitioning and why is it important?
**Q:** Why is partitioning important in big data?  
**A:** It improves parallelism and query performance by dividing data into manageable chunks.

---

## 7. What is the difference between batch and streaming processing?
**Q:** Batch vs streaming processing?  
**A:** Batch processes large volumes of data at intervals, while streaming processes data continuously in near real-time.

---

## 8. What is exactly-once processing?
**Q:** What does exactly-once processing mean?  
**A:** It ensures each record is processed only once, even in the presence of failures or retries.

---

## 9. What is idempotency in data pipelines?
**Q:** What is idempotency?  
**A:** Running the same operation multiple times produces the same result, preventing duplicate data.

---

## 10. What is schema-on-read vs schema-on-write?
**Q:** What is the difference between schema-on-read and schema-on-write?  
**A:** Schema-on-read applies schema when reading data, while schema-on-write enforces schema when writing data.

---

## 11. What is a data lake vs data warehouse?
**Q:** Data lake vs data warehouse?  
**A:** A data lake stores raw data in various formats, while a data warehouse stores structured, processed data optimized for queries.

---

## 12. What is a data lakehouse?
**Q:** What is a data lakehouse?  
**A:** It combines features of data lakes and data warehouses, enabling both raw storage and structured analytics.

---

## 13. What is the Bronze, Silver, Gold architecture?
**Q:** What are Bronze, Silver, Gold layers?  
**A:** Bronze is raw data, Silver is cleaned/transformed data, and Gold is business-ready aggregated data.

---

## 14. What is data orchestration?
**Q:** What is data orchestration?  
**A:** Coordinating and managing workflows and dependencies in data pipelines, often using tools like Azure Data Factory.

---

## 15. What is fault tolerance in distributed systems?
**Q:** What is fault tolerance?  
**A:** The ability of a system to continue functioning even when some components fail.

---

## 16. What is horizontal scalability?
**Q:** What is horizontal scaling?  
**A:** Adding more machines to distribute workload instead of increasing the power of a single machine.

---

## 17. What is a bottleneck in a data pipeline?
**Q:** What is a bottleneck?  
**A:** A point in the system where performance is limited due to resource constraints or inefficient processing.

---

## 18. What is data consistency?
**Q:** What does data consistency mean?  
**A:** Ensuring data remains accurate and consistent across systems and over time.

---

## 19. What is a checkpoint in data processing?
**Q:** What is a checkpoint?  
**A:** A saved state of processing that allows recovery without restarting from the beginning.

---

## 20. How would you design a scalable data pipeline?
**Q:** How do you design a scalable data pipeline?  
**A:** By using distributed systems, partitioning data, ensuring fault tolerance, optimizing transformations, and orchestrating workflows efficiently.