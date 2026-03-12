## Data Pipelines Defined

![Data Pipelines](../imgs/66.png)

In Microsoft Fabric, a **Data Pipeline** is a workflow that **moves and transforms data from multiple sources to a desired destination**. Pipelines help automate data processing tasks and manage the flow of data across different systems within the data platform.

They are commonly used to **extract, transform, and load (ETL) data**, ensuring that data is prepared and available for analytics, reporting, or machine learning.

---

<!-- ## Key Features of Data Pipelines -->
![Key Features of Pipelines](../imgs/67.png)

### 1. Orchestration
Data pipelines orchestrate the **flow of data across different processing steps**, defining the order and logic of the activities required to complete a data workflow.

### 2. Activities
Pipelines can include various **activities**, such as:

- Data extraction from source systems  
- Data transformation and processing  
- ETL or ELT operations  
- Loading data into target systems  

They can also support **advanced operations**, including machine learning workflows and other automated tasks.

### 3. Integration
Data pipelines can integrate with multiple **data sources and services**, including:

- Databases  
- Data lakes  
- APIs  
- Cloud storage systems  

This allows organizations to build **comprehensive and flexible data workflows**.

### 4. Automation
Pipelines enable **automation of data movement and transformation processes**, ensuring that data is **regularly updated and ready for analysis** without manual intervention.

### 5. Monitoring and Management
Microsoft Fabric provides tools to **monitor pipeline executions**, track errors, and manage pipeline performance.  
These capabilities help ensure that **data workflows run reliably and efficiently across the organization**.

---

## Data Pipeline Hits Bug

In the workspace "Human Resources" hit the button "New item" and search for the "Data pipeline" hit the button and add the name "pipeline3".

We have different approch on creating the pipeline, let's go with "Copy data assistant"
![Copy data assistant](../imgs/68.png)

A pop-up will appeare, named "Copy data". The data we want to copy is in "HR_01", click on it
![Copy data assistant](../imgs/69.png)

We can choose to load the tables or the files, we will load the files, the file that we want to load is called "titanic (1).csv" click on it and inspect the file in a tabular format. Verify the data and hit Next.
![Copy data assistant](../imgs/70.png)

Choose a dastinaion where the data is going to be exported, we will choose "ACCTG_DW01"
![Copy data assistant](../imgs/71.png)

Load to new table, call the table "titanic", verify all the data types and click on Source to select all the columns and hit next.
![Copy data assistant](../imgs/72.png)

Choose the option Workspace on the Data store type and hit Next.
Review then save and run the pipeline. 
![Copy data assistant](../imgs/73.png)

And it is goint to crush. We will look at the error and try to solve it.
We will look on the internet for the following error "Yoc can ignore bade data by setting BadDataFound to null".
Check the internet pages and search for the information you need to solve this error.
![Crushed Pipeline](../imgs/74.png)

Click on the pipeline "Copy data" and go to the setting
![Crushed Pipeline](../imgs/75.png)

Change the Escape character from "Backslash (\)" to "Double quotes (")" and hit Enter
![Solved Pipeline](../imgs/76.png)

Now hit run and save and it will succed.

---

## Copy Job Data Pipeline

In the workspace "Human Resources" hit the button "New item" and search for the "Copy job (preview)" hit the button and add the name "copyjob3".

A pop-up will apperare.
Choose the data source "HR_01_DW".
![Copy data](../imgs/77.png)

Select some tables and choose only the "dbo.titanic_DF2" and hit Next.
![Copy data](../imgs/78.png)

Choose the destination "ACCT_01" by clicking on it.
Select the map to destination format and how the file will be names and hit Next.
We can also Edit the column mapping.
![Copy data](../imgs/79.png)

In the Setting we can choose the type of copy.
We will do a full copy.
![Copy data](../imgs/80.png)

Review an hit Save + Run
![Copy data](../imgs/81.png)

The pipeline finished.
![Copy data](../imgs/82.png)

We can check the import/copy of the data in the ACCT_01.
We can look at that copy job from the workspace "Human Resources".
The actual pipeline didn't keeped the column names.
We can edit the pipeline and do another full copy.

---

## Create Store Procedures Pipeline

Let's create some stored procedures to update our table within the confliances of a pipeline.
On the Data Warehouse "ACCTG_DW01" click on the button "New SQL query".
Let's create a stored procedure, select the stored procedure and run it.
![Stored Porc](../imgs/83.png)

After the run we can see the stored procedures in the Stored Procedures folder
![Stored Porc Folder](../imgs/84.png)

---

## Add Stored Procedures to Pipeline

Go on the "pipeline3". In the tab "Activities" click on the icon that is specific to "Stored Procedures"
![Stored Porc Pipeline](../imgs/85.png)

Drag and drop a line from Copy data to Stored Procedure. On the store procedure task select in the "Stored procedure name" the stored procedure that you want to use on the data source, in our case we will first choose "uspChangeSexMale". Rename this stored procedure to "Update to Male"

Add another stored procedure, renamed it to "Update to Female", choose the stored procedure that you want to use, in our case "uspChangeSexFemale". Drag and drop a line from the "Update to Male" to "Update to Female".

On the Tab Run, select the button "Validate" first, and then hit "Run" to Save and run the updated pipeline.
![Update the Stored Porc Pipeline](../imgs/86.png)

Check the table to see if everything runned perfectly!

---

## Schedule a Pipeline

To schedule a pipeline we hit the "Schedule" button on the tab "Home" of the pipeline3.
We can now set up the schedule for the pipeline and hit Apply.
![Schedule Pipeline](../imgs/87.png)

---

## Monitor Pipeline

On the pipeline3 we can view the job history by clicking on the button "View run history" on the "Home" tab.

![Monitor 1 Pipeline](../imgs/88.png)

We can also monitor it by clicking on the "Go to Monitor".

![Monitor 2 Pipeline](../imgs/89.png)