# 🧬 Health & Data Engineering Portfolio

A curated collection of **end-to-end, production-ready** data science and data engineering projects.  
Each project demonstrates real-world pipelines, modeling, explainability, and software-engineering best practices.

## 📁 Projects Overview

### **1. Heart Disease — EDA, Modeling & Explainability**
A complete clinical ML workflow using the UCI Heart Disease dataset.  
Includes full EDA, feature engineering, model comparison, hyperparameter tuning, and an interactive Streamlit prediction app.

**Highlights**
- 7 ML models benchmarked  
- Logistic Regression + Random Forest fully interpreted  
- Coefficients, and EDA visuals  
- Production-ready model artifacts (scaler, feature order, joblib model)  
- Interactive app with probability outputs  

**Repo:** `./health-eda-and-model`

---

### **2. Census Income — Modular ETL Pipeline**
A scalable ETL pipeline engineered using Dask and Parquet, with automated monitoring and reproducible end-to-end execution.

**Highlights**
- Modular ETL architecture (`src/`)  
- Pipeline visualization (Graphviz)  
- Missing value handling, encoding, transformations  
- Large-scale data simulation for stress testing  
- Automated tests + monitoring  
- Benchmarked ML (only for pipeline validation)

**Repo:** `./census-income-etl-pipeline`

---

## 🔧 Tech Stack Summary

| Domain | Tools |
|--------|-------|
| **ML & Modeling** | scikit-learn, Logistic Regression, Random Forest |
| **Data Engineering** | Dask, PyArrow, Parquet, Graphviz, pytest |
| **Visualization** | seaborn, matplotlib, Plotly |
| **Deployment** | Streamlit, bash automation |
| **Environment Management** | requirements.txt |

---

## 🎯 Skills Demonstrated
- End-to-end modeling workflows  
- Production-grade ETL design  
- Data validation & monitoring  
- Explainability  
- Pipeline orchestration & testing  
- Streamlit app development  
- Reproducible research practices  

---

## 🚀 Get Started

Clone and explore:

```bash
git clone https://github.com/just-demi/portfolio
cd portfolio
```
Each project contains its own setup, instructions, and reproducible environment.