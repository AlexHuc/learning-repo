# 🧠 Databricks Flashcards

---

## 1. What is Databricks?
**Q:** What is Databricks?  
**A:** Databricks is a cloud-based data platform built on Apache Spark that provides tools for data engineering, data science, and analytics with managed infrastructure.

---

## 2. What problem does Databricks solve?
**Q:** What problems does Databricks solve?  
**A:** It simplifies big data processing by managing Spark clusters, providing collaborative tools, and integrating data pipelines and analytics in one platform.

---

## 3. What is a Databricks workspace?
**Q:** What is a Databricks workspace?  
**A:** A workspace is the environment where users create notebooks, manage jobs, clusters, and collaborate on data projects.

---

## 4. What is a Databricks cluster?
**Q:** What is a cluster in Databricks?  
**A:** A cluster is a group of virtual machines that run Spark workloads.

---

## 5. What are cluster types in Databricks?
**Q:** What types of clusters exist in Databricks?  
**A:** All-purpose clusters (interactive use) and job clusters (used for scheduled or automated jobs).

---

## 6. What is a Databricks notebook?
**Q:** What is a notebook in Databricks?  
**A:** A notebook is an interactive environment where users write and execute code in languages like Python, SQL, Scala, or R.

---

## 7. What is DBFS?
**Q:** What is DBFS (Databricks File System)?  
**A:** DBFS is a distributed file system that provides access to data stored in cloud storage like S3 or Azure Data Lake.

---

## 8. What is a Databricks job?
**Q:** What is a job in Databricks?  
**A:** A job is a scheduled or triggered task that runs notebooks, scripts, or workflows.

---

## 9. What is a workflow in Databricks?
**Q:** What is a workflow?  
**A:** A workflow is a sequence of tasks that define a data pipeline, including dependencies between jobs.

---

## 10. What is Unity Catalog?
**Q:** What is Unity Catalog?  
**A:** Unity Catalog is a centralized governance layer for managing data access, permissions, and metadata across Databricks.

---

## 11. What is Delta Lake in Databricks?
**Q:** How does Databricks use Delta Lake?  
**A:** Databricks uses Delta Lake as its default storage layer to provide reliable, high-performance data processing.

---

## 12. What is Auto Loader?
**Q:** What is Auto Loader in Databricks?  
**A:** Auto Loader is a tool for incrementally ingesting new data files from cloud storage into Delta tables.

---

## 13. What is a Spark cluster in Databricks?
**Q:** How does Databricks manage Spark clusters?  
**A:** Databricks automatically provisions and scales Spark clusters based on workload requirements.

---

## 14. What is cluster autoscaling?
**Q:** What is autoscaling in Databricks?  
**A:** Autoscaling automatically adjusts the number of worker nodes based on workload demand.

---

## 15. What is caching in Databricks?
**Q:** How does caching work in Databricks?  
**A:** Frequently accessed data is stored in memory to reduce recomputation and improve performance.

---

## 16. What is a job cluster vs all-purpose cluster?
**Q:** What is the difference between job and all-purpose clusters?  
**A:** Job clusters are created for specific jobs and terminated after execution; all-purpose clusters are shared for interactive use.

---

## 17. What is Photon in Databricks?
**Q:** What is Photon?  
**A:** Photon is a high-performance query engine in Databricks that accelerates SQL and DataFrame workloads.

---

## 18. What is Delta Live Tables?
**Q:** What are Delta Live Tables?  
**A:** A framework for building reliable, maintainable, and testable data pipelines with automatic data quality checks.

---

## 19. How does Databricks integrate with cloud storage?
**Q:** How does Databricks connect to S3 or Azure Data Lake?  
**A:** It uses native integrations and credentials to read/write data directly from cloud storage.

---

## 20. Why use Databricks for data engineering?
**Q:** Why is Databricks popular for data engineering?  
**A:** Because it simplifies distributed processing, integrates Spark and Delta Lake, supports scalable pipelines, and provides collaborative tools.