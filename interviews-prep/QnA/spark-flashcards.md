# 🧠 Spark Flashcards

---

## 1. What is Apache Spark?
**Q:** What is Apache Spark?  
**A:** Apache Spark is a distributed data processing framework that enables large-scale data processing across clusters using parallel computation and in-memory execution.

---

## 2. What is the role of the Driver?
**Q:** What does the Driver do in Spark?  
**A:** The Driver is the main program that creates the execution plan, schedules tasks, and coordinates executors.

---

## 3. What are Executors?
**Q:** What are Executors in Spark?  
**A:** Executors are worker processes that run tasks, process data partitions, and return results to the driver.

---

## 4. What is a Partition?
**Q:** What is a partition in Spark?  
**A:** A partition is a chunk of data distributed across the cluster, processed in parallel by tasks.

---

## 5. What is a Task?
**Q:** What is a task in Spark?  
**A:** A task is the smallest unit of work, responsible for processing one partition of data.

---

## 6. What is a Job?
**Q:** What triggers a job in Spark?  
**A:** A job is triggered when an action (e.g., count, show, write) is executed.

---

## 7. What is a Stage?
**Q:** What is a stage in Spark?  
**A:** A stage is a group of tasks that can run in parallel without requiring a shuffle.

---

## 8. What is Lazy Evaluation?
**Q:** What is lazy evaluation in Spark?  
**A:** Transformations are not executed until an action is called, allowing Spark to optimize execution.

---

## 9. Transformations vs Actions
**Q:** What is the difference between transformations and actions?  
**A:** Transformations are lazy and create new datasets; actions trigger execution and return results.

---

## 10. What is an RDD?
**Q:** What is an RDD?  
**A:** A Resilient Distributed Dataset is a low-level distributed collection of data that is fault-tolerant and immutable.

---

## 11. What is a DataFrame?
**Q:** What is a DataFrame in Spark?  
**A:** A DataFrame is a distributed dataset organized into named columns with schema, optimized for performance.

---

## 12. What is a Shuffle?
**Q:** What is a shuffle in Spark?  
**A:** A shuffle is the process of redistributing data across partitions, usually during joins or aggregations.

---

## 13. Why are shuffles expensive?
**Q:** Why is shuffle considered expensive?  
**A:** Because it involves network I/O, disk I/O, and data movement between executors.

---

## 14. What causes data skew?
**Q:** What is data skew?  
**A:** Data skew occurs when data is unevenly distributed across partitions, causing performance bottlenecks.

---

## 15. How do you handle data skew?
**Q:** How can you fix data skew?  
**A:** By using broadcast joins, salting, repartitioning, adaptive query execution, or pre-aggregation.

---

## 16. What is a Broadcast Join?
**Q:** What is a broadcast join?  
**A:** It sends a small dataset to all executors, avoiding shuffle and enabling faster joins.

---

## 17. Repartition vs Coalesce
**Q:** What is the difference between repartition and coalesce?  
**A:** Repartition reshuffles data and increases/decreases partitions; coalesce reduces partitions without full shuffle.

---

## 18. What is DAG?
**Q:** What is a DAG in Spark?  
**A:** A Directed Acyclic Graph represents the sequence of transformations used to optimize execution.

---

## 19. What is caching?
**Q:** What does cache() do in Spark?  
**A:** It stores a dataset in memory to avoid recomputation and improve performance.

---

## 20. What is fault tolerance in Spark?
**Q:** How does Spark achieve fault tolerance?  
**A:** Through lineage (DAG), allowing it to recompute lost partitions if a node fails.