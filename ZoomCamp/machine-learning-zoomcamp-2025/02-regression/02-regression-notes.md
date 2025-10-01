# ML Zoomcamp 2.1 - Car Price Prediction Project

## 📝 Problem Description
- Goal: Build a model to help users of online classified websites set the best price for selling their cars.  
- Input: Car features (make, model, engine type, fuel type, etc.).  
- Output: Predict the **price (MSRP: Manufacturer Suggested Retail Price)** of the car.  


## 📊 Dataset Information
- Source: **Kaggle** (car prices dataset).  
- Features: Manufacturer, model, engine, fuel type, and more.  
- Target variable: **MSRP (price of the car)**.  
- Format: CSV file.  


## 🚀 Project Plan
### Step 1: Data Exploration
- Load dataset.  
- Perform Exploratory Data Analysis (EDA).  

### Step 2: Baseline Model
- Train a **Linear Regression** model on the dataset.  

### Step 3: Model Implementation
- Manually implement linear regression.  
- Understand inner workings of the algorithm.  

### Step 4: Model Evaluation
- Use **RMSE (Root Mean Squared Error)** to measure performance.  

### Step 5: Feature Engineering
- Create new features to improve predictive power.  

### Step 6: Regularization
- Address **numerical stability issues**.  
- Apply **regularization techniques** to improve robustness.  

### Step 7: Final Model
- Refine the pipeline with improvements.  


## 💻 Code Repository
- GitHub repo: **mlbookcamp-code**  
- Chapter: **02-car-price**  
- Contents:
  - Jupyter Notebook with all session code.  
  - CSV dataset file for training.  

---

# ML Zoomcamp 2.2 - Data Preparation
## 🎬 Introduction
- Focus: **Prepare dataset** for the car price prediction project.  
- Data source: GitHub repo → `mlbookcamp-code / chapter-02-car-price`.  
- Files:
  - **CSV dataset** (car price data).  
  - **Jupyter Notebook** (with session code).  


## 📥 Step 1: Download the Dataset
- Options:  
  - Use `wget` to download from GitHub.  
  - Or download manually via browser (Save As).  
- Save the dataset locally for further processing.  


## 📂 Step 2: Load the Dataset
- Use **pandas `read_csv()`** to load the dataset.  
- Inspect with `.head()` to see first 5 rows.  
- Data contains car features (make, model, year, engine type, transmission, etc.) and **target variable: MSRP (Manufacturer Suggested Retail Price)**.  


## 🧹 Step 3: Clean Column Names
- Issues:
  - Inconsistent capitalization.  
  - Spaces vs. underscores.  
- Solution:
  - Convert all column names to **lowercase**.  
  - Replace spaces with **underscores**.  
- Done via pandas `df.columns.str.lower().str.replace(" ", "_")`.  


## 🧽 Step 4: Normalize String Values
- Problem: Some categorical values inconsistent (UPPERCASE vs. lowercase).  
- Approach:
  - Identify **string columns** using `df.dtypes`.  
  - Select columns of type **object**.  
  - Convert them all to lowercase and replace spaces with underscores.  


## ✅ Result After Cleaning
- Dataset is now:  
  - Uniform column names.  
  - Consistent categorical values.  
- Easier to work with in future steps (e.g., feature engineering, modeling).  

---

#


