
# **EcoFin 360** - *Economic and Financial Insights Platform*

A comprehensive platform to automate data pipelines, analyze trends in economics and finance, and perform predictive modeling to support informed decision-making.

---

## **Project Goals**

**EcoFin 360 empowers users to:**
  
1. **Analyze macroeconomic data** (e.g., GDP, inflation, unemployment rates).  
2. **Monitor and optimize stock portfolios** for performance and risk.  
3. **Predict economic indicators** and **forecast stock price trends**.

---

## **Platform Architecture**

EcoFin 360 employs the **Bronze-Silver-Gold Architecture** for efficient and streamlined data management:

### 1. **Bronze Layer**  
- Stores **raw, unprocessed data** from external sources (e.g., APIs, files).  
- Data is saved in **MinIO** in formats such as JSON and CSV.

### 2. **Silver Layer**  
- Contains **cleaned and structured data** prepared for analysis.  
- Managed using **Delta Lake** to ensure ACID compliance and reliability.

### 3. **Gold Layer**  
- Hosts **analytics-ready data** optimized for dashboards and advanced modeling tasks.  
- Data is stored in **PostgreSQL** for fast querying and integration.

<p align="center">  
   <img src="docs/architecture_diagram.png" alt="Architecture Diagram" width="70%">  
</p>

---

## **Key Use Cases**

### 1. **Economic Analysis**  
- Study **GDP growth** and **inflation trends**.  
- Analyze the **relationship between macroeconomic variables and market performance**.

### 2. **Portfolio Management**  
- Track **portfolio performance metrics**, including returns and volatility.  
- Optimize portfolios using **Modern Portfolio Theory (MPT)**.

### 3. **Predictive Modeling**  
- **Forecast economic indicators**, such as GDP growth or stock price trends.  
- Analyze and simulate **portfolio risks under various scenarios**.

---
