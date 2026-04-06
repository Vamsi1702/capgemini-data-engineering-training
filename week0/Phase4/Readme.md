# 📊 Phase 4 — Mini Project: Business Pipeline & Analytics  
**Capgemini Data Engineering Training — Week 0, Phase 4**

## 🚀 Project Overview  
In this phase, I built an end-to-end business data pipeline using **PySpark**. The workflow includes data loading, cleaning, transformation, joining, and saving to generate business insights. The project was developed using Spark Playground.

---

## 📁 Datasets Used  

| Dataset        | Description |
|----------------|------------|
| customers.csv  | Customer details (customer_id, name, city, state, zip_code) |
| sales.csv      | Transaction data (sale_id, customer_id, product_id, date, quantity, total_amount) |

---

## 🛠️ Technologies Used  
- PySpark  
- Spark SQL  
- Spark Playground  
- GitHub  

---

## 🔄 ETL Pipeline Structure  

### Extract  
Loaded datasets from `/samples/` into DataFrames and explored schema using `show()` and `printSchema()`.

### Transform  
- Cleaned data (removed nulls, fixed data types)  
- Joined datasets on `customer_id`  
- Applied aggregations and business logic using `groupBy`, `agg`, `when`, `withColumn`

### Load  
Saved final output to `/tmp/report` in CSV format using overwrite mode.

---

## 📌 Tasks Completed  

| Task | Business Question | Output |
|------|------------------|--------|
| 1 | Total revenue per day | sale_date, total_sales |
| 2 | City with highest revenue | city, total_revenue |
| 3 | Top 5 customers | first_name, total_spend |
| 4 | Customers with >1 order | customer_id, order_count |
| 5 | Customer segmentation | name, spend, segment |
| 6 | Final reporting table | combined insights |
| 7 | Save output | CSV file |

---

## 📚 New Concepts Learned  

| Concept | Learning |
|--------|---------|
| `when()` | Conditional logic like CASE WHEN |
| `withColumn()` | Add/modify columns |
| `limit()` | Top N results |
| `write.mode()` | overwrite, append, ignore, error |
| `_sum` | Alias for PySpark sum |
| HAVING vs WHERE | Filter before vs after aggregation |

---

## ⚠️ Difficulties Faced  
- Confusion in JOIN usage → used only when data is in multiple tables  
- `sum` vs `_sum` conflict → resolved using alias  
- Write errors → `/samples` is read-only, used `/tmp`  
- Naming inconsistency → standardized DataFrame names  
- `withColumn` + `when` confusion → practiced SQL to PySpark conversion  

---

## 🎯 Key Learnings  
- Built a complete ETL pipeline from raw data to final output  
- Learned joins, aggregations, and conditional transformations  
- Understood importance of data cleaning before processing  
- Improved by converting SQL logic into PySpark  
- Gained real-world analytics insights (revenue, segmentation, top customers)  

---

## 🏁 Conclusion  
This project demonstrates how real-world data pipelines are built and used to generate business insights through structured data processing.
