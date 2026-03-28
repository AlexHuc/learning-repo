# The order of operations in SQL

## ✅ SQL Logical Order:
```sql
1. FROM           → Choose the main table
2. JOIN           → Combine tables
3. WHERE          → Filter rows (before aggregation)
4. GROUP BY       → Group rows for aggregation
5. HAVING         → Filter groups (after aggregation)
6. SELECT         → Choose columns to display
7. DISTINCT       → Remove duplicates
8. ORDER BY       → Sort results
9. LIMIT / OFFSET → Restrict number of rows
```

### 🔥 Easy Way to Remember
```sql
FJ → W → G → H → S → O → L
```

---

# What is a Primary Key?

**`Primary Key `** &rarr; a column (or set of columns) that uniquely identifies each row in a table

## 🔧 Example
```
Customers Table
------------------------
customer_id | name
------------------------
1           | Alice
2           | Bob
```
👉 customer_id = primary key

## 🎯 Characteristics
- Unique (no duplicates)
- Not NULL
- One primary key per table

💡 Use Case
- Identify records
- Join tables

---

# 🔗 Main Types of SQL Joins
## 1️⃣ INNER JOIN
**`INNER JOIN`** &rarr; Returns only rows that have matching values in both tables

```sql
SELECT *
FROM orders o
INNER JOIN customers c
ON o.customer_id = c.customer_id;
```

## 2️⃣ LEFT JOIN
**`LEFT JOIN`** &rarr; Returns all rows from the left table + matching rows from the right table

```sql
SELECT *
FROM customers c
LEFT JOIN orders o
ON c.customer_id = o.customer_id;
```

## 3️⃣ RIGHT JOIN
**`RIGHT JOIN`** &rarr; Returns all rows from the right table + matching rows from the left

```sql
SELECT *
FROM orders o
RIGHT JOIN customers c
ON o.customer_id = c.customer_id;
```

## 4️⃣ FULL OUTER JOIN
**`FULL OUTER JOIN`** &rarr; Returns all rows from both tables, with NULLs where no match exists

```sql
SELECT *
FROM customers c
FULL OUTER JOIN orders o
ON c.customer_id = o.customer_id;
```

## 5️⃣ CROSS JOIN
**`CROSS JOIN`** &rarr; Returns the Cartesian product (all combinations of rows)

```sql
SELECT *
FROM table1
CROSS JOIN table2;
```

## 6️⃣ SELF JOIN
**`SELF JOIN`** &rarr; A table joined with itself

```sql
SELECT a.name, b.name
FROM employees a
JOIN employees b
ON a.manager_id = b.employee_id;
```

## 7️⃣ SEMI JOIN (EXISTS)
**`SEMI JOIN (EXISTS)`** &rarr; Returns rows from left table where a match exists in right table

```sql
SELECT *
FROM customers c
WHERE EXISTS (
  SELECT 1
  FROM orders o
  WHERE c.customer_id = o.customer_id
);
```

## 8️⃣ ANTI JOIN (NOT EXISTS)
**`ANTI JOIN (NOT EXISTS)`** &rarr; Returns rows that do NOT have a match

```sql
SELECT *
FROM customers c
WHERE NOT EXISTS (
  SELECT 1
  FROM orders o
  WHERE c.customer_id = o.customer_id
);
```

## ⚖️ Summary Table
| Join Type | Returns                  |
| --------- | ------------------------ |
| INNER     | Matching rows only       |
| LEFT      | All left + matches       |
| RIGHT     | All right + matches      |
| FULL      | All rows from both       |
| CROSS     | All combinations         |
| SELF      | Table joined with itself |
| SEMI      | Rows with match          |
| ANTI      | Rows without match       |

---

# UNION, UNION ALL and INTERSECT
## 1️⃣ UNION
**`UNION`** &rarr; Combines results from two queries and **removes duplicates**

## 2️⃣ UNION ALL
**`UNION ALL`** &rarr; Combines results from two queries and **keeps duplicates**

## 3️⃣ INTERSECT
**`INTERSECT`** &rarr; **Returns only the rows that exist in both queries**

---

# 🧠 What is a Dimension Table?

**`Dimension Table`** &rarr; contains **descriptive information** used to categorize or filter data

## 🔧 Example
```
Dim_Customer
----------------------------
customer_id | name | country
----------------------------
1           | Alice | RO
2           | Bob   | UK
```

## 🎯 Characteristics
- Descriptive data
- Used for filtering and grouping
- Usually smaller tables

## 💡 Examples
- customers
- products
- dates

---


# 🧠 What is a Fact Table?
**`Fact Table`** &rarr; contains measurable, quantitative data (**metrics**)

## 🔧 Example
```
Fact_Sales
-----------------------------------
sale_id | customer_id | amount
-----------------------------------
1       | 1           | 100
2       | 2           | 200
```

## 🎯 Characteristics
- Contains metrics (numbers)
- References dimension tables
- Usually very large

## 💡 Examples
- sales
- transactions
- clicks

---

# CTE vs Temp Table
## 🧠 What is a CTE (Common Table Expression)?

**`CTE`** &rarr; a temporary result of a query

### 🎯 Example
```sql
WITH high_salary AS (
    SELECT * FROM employees WHERE salary > 5000
)
SELECT * FROM high_salary;
```

### 🧠 Key Characteristics
- Exists only during query execution
- Improves readability
- Can be reused in the same query
- Not stored physically 
- Stored in-momory
- Can be RECURSIVE

---

## 🧠 What is a Temporary Table?
**`Temporary Table`** &rarr; a physical table stored temporarily in the database, used across multiple queries in a session

### 🔧 Syntax (SQL Server example)
```sql
CREATE TABLE #temp_table (
    id INT,
    name VARCHAR(50)
);
```

### 🎯 Example
```sql
CREATE TABLE #high_salary AS
SELECT * FROM employees WHERE salary > 5000;

SELECT * FROM #high_salary;
```

### 🧠 Key Characteristics
- Exists for the duration of the session
- Stored physically (in temp storage)
- Can be indexed
- Can be reused across multiple queries

---

## ⚖️ CTE vs Temp Table
| Feature     | CTE         | Temp Table         |
| ----------- | ----------- | ------------------ |
| Storage     | Not stored  | Stored in DB       |
| Lifetime    | One query   | Session            |
| Performance | Recomputed  | Can be reused      |
| Indexing    | No          | Yes                |
| Use case    | Readability | Reuse / large data |

### 🎯 When to Use Each
✅ Use CTE when:
- Query is simple
- You need readability
- Used once

✅ Use Temp Table when:
- Data is reused multiple times
- Large datasets
- Need indexing
- Performance matters

---

# Index
## 🧠 What is an Index in SQL?

**`INDEX`** &rarr; a data structure that improves the speed of data retrieval operations on a table

### 🔧 Simple Explanation

Without index:
- 👉 Database scans the entire table (full table scan) ❌

With index:
- 👉 Database jumps directly to the data ✅

### 📚 Real-Life Analogy

📖 Book index
- You don’t read the whole book
- You go to the index → find page → jump there

👉 SQL index works the same way

### ⚙️ How It Works

An index stores:
- column values
- pointers to actual rows

👉 So queries don’t scan everything

### 🔧 Example
Table:
```
Customers
------------------------
id | name | country
```

Create index:
```sql
CREATE INDEX idx_country
ON customers(country);
```

Query:
```
SELECT * FROM customers WHERE country = 'RO';
```

👉 With index:
- fast lookup

👉 Without index:
- scan entire table

### 🎯 When Index Helps
- WHERE clauses
- JOIN conditions
- ORDER BY
- GROUP BY

### ⚠️ Trade-Offs (VERY IMPORTANT)

❌ Downsides:
- take extra storage
- slow down INSERT / UPDATE / DELETE

👉 Because index must be updated too

---

## 🧩 Types of Indexes
### 1️⃣ Clustered Index
**`Clustered Index`** &rarr; Defines the physical order of data
- Only one per table
- Data is stored in that order

### 2️⃣ Non-Clustered Index
**`Non-Clustered Index`** &rarr; Separate structure from table
- Multiple allowed
- Points to actual data

### 3️⃣ Composite Index
```sql
CREATE INDEX idx_name_country
ON customers(name, country);
```
👉 Index on multiple columns

## 🧠 Important Concept
Index improves read performance, but can hurt write performance

---

# Normalization
## 🧠 What is Normalization?
**`Normalization`** &rarr; the **process of organizing data in a database** to reduce redundancy and improve data integrity.

## 🔧 Simple Explanation
👉 Instead of repeating data:
```
❌ Bad (not normalized)
Customer | Address | Order
Alice    | RO      | 1
Alice    | RO      | 2
```

👉 You split it into:
```
✅ Normalized
Customers
Alice | RO

Orders
Order1 | Alice
Order2 | Alice
```

## 🎯 Goals of Normalization
- eliminate duplicate data
- ensure consistency
- improve data integrity
- make updates easier

---

## 🧩 Normal Forms (Levels of Normalization)
### 1️⃣ First Normal Form (1NF)
**`1NF`** &rarr; No repeating groups, atomic values only

❌ Bad
```
Customer | Orders
Alice    | 1,2,3
```

✅ Good
```
Customer | Order
Alice    | 1
Alice    | 2
```

### 2️⃣ Second Normal Form (2NF)
**`2NF`** &rarr; No partial dependency (all columns depend on full primary key)

👉 Applies when:
- composite key exists

### 3️⃣ Third Normal Form (3NF)
**`3NF`** &rarr; No transitive dependency

❌ Bad
```
Customer | City | Country
```
If:
- City &rarr; Country

👉 Redundant ❌

✅ Good
```
Customer | City
City     | Country
```

## 🧠 Key Idea
Each piece of data should be stored only once

## ⚖️ Normalization vs Denormalization
| Feature     | Normalization | Denormalization |
| ----------- | ------------- | --------------- |
| Redundancy  | Low           | Higher          |
| Performance | Slower joins  | Faster queries  |
| Use case    | OLTP systems  | Data warehouses |

### 🧠 Important Insight (Interview Gold)
👉 In data warehouses:
- we often denormalize (star schema)

👉 In transaction systems:
- we normalize

---

# Denormalization
## 🧠 What is Denormalization?

**`Denormalization`** &rarr; the **process of combining data into fewer tables to improve query performance**, even if it introduces some redundancy

## 🔧 Simple Explanation

Instead of splitting data into many tables (normalized):
👉 You combine them to avoid joins

❌ Normalized (many tables)
```
Customers
Orders
Products
```

✅ Denormalized (combined)
```
Sales_Table
-------------------------------------
customer_name | product | amount
Alice         | Phone   | 100
```

👉 Easier to query, fewer joins

## 🎯 Why Denormalization Exists

Normalization:
- good for data integrity
- bad for performance (many joins)

Denormalization:
- improves query speed
- reduces joins
- simplifies analytics

## ⚙️ Where It’s Used

👉 Mainly in:
- Data Warehouses
- Lakehouse architectures
- BI systems

## Important

👉 Denormalization is used in:
⭐ Star Schema
```
Fact_Sales
   ↓
Dim_Customer
Dim_Product
```
👉 Dimensions are often slightly denormalized

## ⚠️ Trade-Offs

❌ Downsides
- data duplication
- risk of inconsistency
- more storage

✅ Benefits
- faster queries
- simpler reporting
- better performance

---

# Star Schema
## 🧠 What is a Star Schema?

**`Star Schema`** &rarr; a **data modeling technique** where a **central fact table is connected to multiple dimension tables**, forming a star-like structure.

## ⭐ Why is it called “Star”?

Because the structure looks like a star:
```
          Dim_Customer
                |
Dim_Product — Fact_Sales — Dim_Date
                |
           Dim_Store
```
👉 Fact table in the center
👉 Dimension tables around it

## 🧩 Components of Star Schema
### 1️⃣ Fact Table (Center)
- metrics (numbers)
- foreign keys to dimensions

📦 Example
```
Fact_Sales
-----------------------------------
sale_id | customer_id | product_id | amount
-----------------------------------
1       | 101         | 200        | 100
```

#### 🎯 Characteristics
- large table
- numeric data
- used for analysis

### 2️⃣ Dimension Tables (Surrounding)
- descriptive attributes

📦 Example
```
Dim_Customer
------------------------
customer_id | name | country
------------------------
101         | Alice | RO
Dim_Product
------------------------
product_id | name | category
```

#### 🎯 Characteristics
- smaller tables
- used for filtering, grouping

## ⚙️ How It Works

👉 You join fact + dimensions:
```sql
SELECT c.name, SUM(f.amount)
FROM Fact_Sales f
JOIN Dim_Customer c ON f.customer_id = c.customer_id
GROUP BY c.name;
```

## 🎯 Why Use Star Schema?
1. ✅ Simplicity
   - easy to understand
   - easy to query
2. ✅ Performance
   - fewer joins
   - faster queries
3. ✅ Optimized for BI
   - perfect for dashboards

### ⚖️ Star Schema vs Normalized Model
| Feature     | Star Schema | Normalized |
| ----------- | ----------- | ---------- |
| Structure   | Simple      | Complex    |
| Joins       | Few         | Many       |
| Performance | Fast        | Slower     |
| Redundancy  | Higher      | Lower      |

## 🎯 One-Line Summary
**Star schema = fact table + dimension tables (like a star)**

---

# Snowflake Schema
## 🧠 What is a Snowflake Schema?

**`Snowflake Schema`** &rarr; **data modeling technique** where **dimension tables are normalized into multiple related tables**, forming a structure similar to a snowflake

## ❄️ Why is it called “Snowflake”?

Because the structure branches out like this:
```
Fact_Sales
   |
Dim_Customer
   |
Dim_Country
   |
Dim_Region
```
👉 Dimensions are split into multiple tables &rarr; looks like a snowflake ❄️

## 🧩 Components
### 1️⃣ Fact Table

Same as in star schema:
```
Fact_Sales
-------------------------------
sale_id | customer_id | amount
```

### 🔵 2️⃣ Normalized Dimension Tables

Instead of one flat table:

Star Schema (denormalized)
```
Dim_Customer
------------------------
customer_id | name | country | region
```

Snowflake Schema (normalized)
```
Dim_Customer
------------------------
customer_id | name | country_id
```
```
Dim_Country
------------------------
country_id | country_name | region_id
```
```
Dim_Region
------------------------
region_id | region_name
```
👉 Data is split into multiple related tables

## ⚙️ How It Works

To get full data:
```sql
SELECT c.name, r.region_name
FROM Fact_Sales f
JOIN Dim_Customer c ON f.customer_id = c.customer_id
JOIN Dim_Country co ON c.country_id = co.country_id
JOIN Dim_Region r ON co.region_id = r.region_id;
```
👉 More joins needed

## 🎯 Why Use Snowflake Schema?
✅ Advantages
- Less data redundancy
- Better data integrity
- Smaller storage size

❌ Disadvantages
- More joins &rarr; slower queries
- More complex structure
- Harder to understand

### ⚖️ Star vs Snowflake Schema
| Feature     | Star Schema  | Snowflake Schema |
| ----------- | ------------ | ---------------- |
| Structure   | Simple       | Complex          |
| Dimensions  | Denormalized | Normalized       |
| Joins       | Few          | More             |
| Performance | Faster       | Slower           |
| Storage     | More         | Less             |

### 🧠 When to Use Snowflake Schema
- data consistency is critical
- dimensions are large and complex
- storage optimization matters

## 🎯 One-Line Summary
**Snowflake = normalized dimensions, more joins**

---

# Stored Procedure
## 🧠 What is a Stored Procedure?
**`Stored Procedure`** &rarr; a **precompiled SQL script stored in the database that can be executed multiple times**

## 🔧 Example
```sql
CREATE PROCEDURE GetCustomers
AS
SELECT * FROM Customers;
```

## 🎯 Characteristics
- reusable
- improves performance (precompiled)
- can accept parameters
- used for business logic

---

# Trigger
## 🧠 What is a Trigger?
**`Trigger`** &rarr; a piece of **code that automatically executes when a specific event occurs** (INSERT, UPDATE, DELETE)

## 🔧 Example
```sql
CREATE TRIGGER trg_after_insert
ON Orders
AFTER INSERT
AS
PRINT 'New order inserted';
```

## 🎯 Characteristics
- runs automatically
- tied to table events
- used for auditing, validation

### ⚖️ Stored Procedure vs Trigger
| Feature   | Stored Procedure | Trigger           |
| --------- | ---------------- | ----------------- |
| Execution | Manual           | Automatic         |
| Use case  | Business logic   | Event-based logic |

--- 

# UDF (User-Defined Function)
## 🧠 What is a UDF?
**`UDF (User-Defined Function)`** a **custom function created** by the user to perform specific logic that is not available in built-in functions

## 🔧 Simple Explanation
SQL already has functions like:
- `SUM()`
- `COUNT()`
- `UPPER()`

👉 But if you need custom logic:
➡️ You create a UDF

--- 

## 🧩 Types of UDFs
### 1️⃣ Scalar UDF
**`Scalar UDF`** &rarr; Returns a single value

### 2️⃣ Table-Valued UDF
**`Table-Valued UDF`** &rarr; Returns a table

---

# 🧠 SQL Data Types Overview

| Category        | Data Type        | Description | Example | Notes |
|----------------|------------------|------------|---------|------|
| 🔢 Numeric      | INT              | Integer numbers | 10 | Commonly used for IDs |
| 🔢 Numeric      | BIGINT           | Large integers | 10000000000 | For very large values |
| 🔢 Numeric      | DECIMAL(p,s)     | Exact numbers with precision | 10.25 | Used for money |
| 🔢 Numeric      | FLOAT            | Approximate numbers | 10.2567 | May lose precision |
| 🔤 String       | CHAR(n)          | Fixed-length string | 'ABC     ' | Padded with spaces |
| 🔤 String       | VARCHAR(n)       | Variable-length string | 'ABC' | Stores only used space |
| 🔤 String       | TEXT             | Large text data | Long paragraph | DB-specific support |
| 📅 Date/Time    | DATE             | Stores date only | 2025-01-01 | No time component |
| 📅 Date/Time    | TIME             | Stores time only | 12:30:00 | No date component |
| 📅 Date/Time    | DATETIME         | Date and time | 2025-01-01 12:30:00 | Common in SQL Server |
| 📅 Date/Time    | TIMESTAMP        | Date and time (auto-updated) | 2025-01-01 12:30:00 | DB-dependent behavior |
| 🧾 Boolean      | BOOLEAN / BIT    | True/False values | 1 / 0 | BIT used in SQL Server |
| 📦 Binary       | BINARY           | Fixed-length binary data | 0xAF12 | Stores raw bytes |
| 📦 Binary       | VARBINARY        | Variable-length binary | 0xAF12 | More flexible |
| 🌍 Unicode      | NCHAR(n)         | Fixed-length Unicode string | 'ă' | Supports international chars |
| 🌍 Unicode      | NVARCHAR(n)      | Variable-length Unicode | 'ă' | Preferred for multi-language |

## 🎯 Quick Notes (Good for Interview)
```sql
DECIMAL vs FLOAT      → precision vs performance
CHAR vs VARCHAR       → fixed vs variable
VARCHAR vs NVARCHAR   → ASCII vs Unicode
DATETIME vs TIMESTAMP → depends on DB behavior
```

---

# 🧠 🧩 Big Picture
When multiple transactions run at the same time, problems can occur
👉 Isolation levels control **how transactions see each other’s data**

# 🔐 Isolation Levels (from weakest &rarr; strongest)
## 1️⃣ READ UNCOMMITTED
**`READ UNCOMMITTED`** &rarr; Can read data that has NOT been committed yet

### ⚠️ Problem
- 👉 Dirty reads possible

### 📦 Example
- Transaction A updates value &rarr; not committed
- Transaction B reads it

👉 If A rolls back &rarr; B read invalid data ❌

## 2️⃣ READ COMMITTED
**`READ COMMITTED`** &rarr; Can only read committed data

### 🎯 Characteristics
- no dirty reads
- still allows inconsistencies

### ⚠️ Problem
- 👉 Non-repeatable reads

## 3️⃣ REPEATABLE READ
**`REPEATABLE READ`** &rarr; Ensures that if you read a row, it won’t change during the transaction

### 🎯 Characteristics
- no dirty reads
- no non-repeatable reads

### ⚠️ Problem
- 👉 Phantom reads still possible

## 4️⃣ SERIALIZABLE
**`SERIALIZABLE`** &rarr; Highest isolation level — behaves like transactions run one after another

### 🎯 Characteristics
- prevents all anomalies
- safest

### ⚠️ Trade-off
- slow
- heavy locking

## 5️⃣ SNAPSHOT (very important modern concept)
**`SNAPSHOT`** &rarr; Each transaction sees a consistent snapshot of data at a point in time

### 🎯 Characteristics
- no locks for reads
- uses versioning

### 💡 Used in:
- modern databases
- Delta Lake
- MVCC systems

---

## ⚠️ Common Problems (Anomalies)
### 1️⃣ Dirty Read
**`Dirty Read`** &rarr; Reading uncommitted data

#### 📦 Example
- A updates &rarr; not committed
- B reads it

👉 If A rolls back → B read invalid data ❌

### 2️⃣ Non-Repeatable Read
**`Non-Repeatable Read`** &rarr; Same query returns different results within a transaction

#### 📦 Example
- A reads value = 100
- B updates to 200
- A reads again &rarr; 200

👉 Inconsistent ❌

### 3️⃣ Phantom Read
**`Phantom Read`** &rarr; New rows appear between reads

#### 📦 Example
- A queries: “count users” &rarr; 10
- B inserts new user
- A queries again &rarr; 11

👉 “phantom” row appears

## ⚖️ Isolation vs Problems
| Isolation Level  | Dirty Read | Non-repeatable | Phantom                    |
| ---------------- | ---------- | -------------- | -------------------------- |
| Read Uncommitted | ✅          | ✅              | ✅                          |
| Read Committed   | ❌          | ✅              | ✅                          |
| Repeatable Read  | ❌          | ❌              | ✅                          |
| Serializable     | ❌          | ❌              | ❌                          |
| Snapshot         | ❌          | ❌              | ❌ (handled via versioning) |

## 🧠 Key Insight (VERY IMPORTANT)
👉 Higher isolation:
- safer
- slower

👉 Lower isolation:
- faster
- riskier

---

# FUNCTIONS & COMMANDS

---

## COALESCE
**`COALESCE`** &rarr; r**eturns the first non-NULL value from a list** of expressions

```sql
COALESCE(value1, value2, value3, ...)
```

---

## ISNULL
**`ISNULL`** &rarr; **replaces NULL with a specified value**

```sql
ISNULL(expression, replacement)
```

### ⚠️ Important
- Only takes 2 arguments
- Database-specific (not standard SQL)

---

## NULLIF
**`NULLIF`** &rarr; **returns NULL if two expressions are equal, otherwise returns the first expression**

```sql
NULLIF(value1, value2)
```

---

## @@TRANCOUNT
**`@@TRANCOUNT`** &rarr; returns the number of active transactions for the current session

### 🔧 Example
```sql
BEGIN TRAN;
SELECT @@TRANCOUNT; -- 1
```

### 🎯 Use Case
- track nested transactions

---

## SAVEPOINT
**`SAVEPOINT`** &rarr; allows you to p**artially roll back** a transaction **to a specific point**

### 🔧 Example
```sql
BEGIN TRAN;

SAVE TRAN save1;

-- some operations

ROLLBACK TRAN save1;
```

### 🎯 Use Case
- rollback only part of a transaction

---

## XACT_ABORT
**`XACT_ABORT`** &rarr; ontrols whether a transaction is automatically rolled back when an error occurs

### 🔧 Example
```sql
SET XACT_ABORT ON;
```

### 🎯 Behavior
| Setting | Behavior                               |
| ------- | -------------------------------------- |
| ON      | Entire transaction rolls back on error |
| OFF     | May continue after error               |

---

## XACT_STATE()
**`XACT_STATE()`** &rarr; returns the current state of a transaction

### 🔧 Possible Values
| Value | Meaning                       |
| ----- | ----------------------------- |
| 1     | Active and valid              |
| 0     | No active transaction         |
| -1    | Uncommittable (must rollback) |

### 🔧 Example
```sql
SELECT XACT_STATE();
```

### 🎯 Use Case
- decide whether to commit or rollback

#### 🧠 How They Work Together (VERY IMPORTANT)
```sql
BEGIN TRAN
   ↓
SAVEPOINT
   ↓
Error occurs
   ↓
Check XACT_STATE()
   ↓
ROLLBACK or COMMIT
```

---

## DELETE CASCADE
**`DELETE CASCADE`** &rarr; automatically deletes related rows in child tables when a row in the parent table is deleted

---

## DEADLOCK
**`DEADLOCK`** &rarr; occurs when two transactions are waiting on each other and neither can proceed

### 🔧 Example
- Transaction A locks row 1 &rarr; waits for row 2
- Transaction B locks row 2 &rarr; waits for row 1

👉 Both stuck ❌

### 🎯 Solution
- database kills one transaction
- use proper locking strategy

---

## Candidate Key
**`Candidate Key`** &rarr; is a column (or set of columns) that can uniquely identify a row

---

## Window Functions
**`Window functions`** &rarr; perform calculations across a set of rows related to the current row, without grouping them

### 🔧 Syntax
```sql
FUNCTION() OVER (PARTITION BY ... ORDER BY ...)
```

### 🎯 Key Idea
- keeps all rows
- adds computed values

---

## ROW_NUMBER()
**`ROW_NUMBER()`** &rarr; Assigns a unique number to each row

### 🔧 Example
```sql
SELECT name, ROW_NUMBER() OVER (ORDER BY salary)
FROM employees;
```

### 🎯 Result
- Alice &rarr; 1  
- Bob &rarr; 2  

---

## RANK()
**`RANK()`** &rarr; Assigns rank with gaps for ties

### 📦 Example
```sql
Salary: 100, 100, 90
Rank:   1,   1,   3
```

---

## DENSE_RANK()
**`DENSE_RANK()`** &rarr; Assigns rank without gaps

### 📦 Example
```
Salary: 100, 100, 90
Rank:   1,   1,   2
```

---

## FIRST_VALUE()
**FIRST_VALUE()** &rarr; Returns the first value in a partition

### 🔧 Example
```sql
FIRST_VALUE(salary) OVER (PARTITION BY dept ORDER BY salary DESC)
```
👉 highest salary in department

---

## LAST_VALUE()
**`LAST_VALUE()`** &rarr; Returns the last value in a partition

---

## LEAD()
**`LEAD()`** &rarr; Returns value from the next row

---

## LAG()
**`LAG()`** &rarr; Returns value from the previous row

---