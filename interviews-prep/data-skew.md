## 🧠 What is Data Skew?

**`Data skew`** &rarr; **data is unevenly distributed across partitions**, causing some tasks to process **much more data than others**.

## ⚙️ Why Data Skew Is a Problem

<u>Ideally</u>:
```shell
Partition 1 → 1M rows  
Partition 2 → 1M rows  
Partition 3 → 1M rows  
```

<u>Skew</u>:
```shell
Partition 1 → 1M rows  
Partition 2 → 1M rows  
Partition 3 → 100M rows  ❌
```

<u>👉 Result</u>:
- One task runs **much longer**
- Other executors stay idle
- Job becomes slow

This is called a **straggler task** problem

---

## 🔥 Where Data Skew Happens Most

### 1. Joins (MOST COMMON)
<u>Example</u>:
```python
orders.join(customers, "customer_id")
```

<u>If one `customer_id` appears millions of times</u>:
- That partition becomes huge
- One executor gets overloaded

### 2. GroupBy / Aggregations
<u>Example</u>:
```python
df.groupBy("country").count()
```

<u>If</u>:
- "US" = 90% of data
- Others = 10%
<u>Then</u>:
- One partition handles most of the work

### 3. Repartitioning by Skewed Key

If one user has massive data &rarr; skew

<u>Example</u>:
```python
df.repartition("user_id")
```

---

## 🔍 How to Detect Data Skew

<u>Signs in Spark UI</u>:
- One task takes much longer
- Uneven task durations
- One executor overloaded

---

## 🛠️ How to Fix Data Skew

### 1️⃣ Broadcast Join

**`Broadcast Join`** &rarr; **sends a small table to all executors, instead of shuffling both datasets**

<u>Normally, Spark does this</u>:
```shell
Large DF → shuffle  
Small DF → shuffle  
→ join
```
<u>With broadcast</u>:
```shell
Small DF → copied to every executor  
Large DF → stays partitioned  
→ join locally on each executor
```

🔹 Why it fixes skew
- Skew happens during **shuffle**
- Broadcast eliminates shuffle for one side
- Each executor joins locally → balanced work

🔹 When to use
- One dataset is small enough to fit in memory
- Typical threshold: < 100–500 MB (depends on cluster)

### 2️⃣ Salting Technique

**`Salting Technique`** &rarr; **A technique to artificially split a skewed key into multiple keys**

<u>Problem</u>:
```shell
user_id = 123 → 100M rows
```
All go to one partition &rarr; **bottleneck** ❌

<u>Solution</u>:
- Add a random suffix:
```shell  
user_id_1  
user_id_2  
user_id_3  
```
- Now data is spread:
```shell
Partition 1 → user_id_1  
Partition 2 → user_id_2  
Partition 3 → user_id_3
```

🔹 Why it fixes skew
- Breaks a “hot key” into multiple keys
- Distributes load across executors

🔹 When to use
- One key is extremely skewed
- Broadcast is not possible (both tables large)

---

## 3️⃣ Repartitioning

**`Repartitioning`** &rarr; **Changing the number of partitions in a dataset**

🔹 Repartition vs Coalesce
| Method      | Shuffle | Use case            |
| ----------- | ------- | ------------------- |
| repartition | Yes     | increase partitions |
| coalesce    | No      | decrease partitions |

🔹 When to use
- Too few partitions
- Uneven data distribution (but not extreme skew)

### Skew Join Optimization (Adaptive Query Execution)

**Spark feature that automatically detects and fixes skew during runtime**
