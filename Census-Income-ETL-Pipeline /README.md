# 📊 Census Income ETL Pipeline — Modular ETL + Monitoring

A fully modular, reproducible ETL pipeline with monitorin and This project demonstrates a production-ready, scalable ETL pipeline for the UCI Census Income dataset.
It uses Dask, Parquet, automated monitoring, and pytest tests to ensure correctness and reproducibility.

The goal of this project is NOT to build the best predictive model.
Instead, the focus is:

### 🎯 Primary Goal

Build a robust, scalable ETL system capable of preparing very large tabular datasets using:

* Data simulation (expansion + controlled jitter) to represent realistic large-scale data
* Missing-value strategy
* Categorical encoding
* Feature transforms
* Partitioned Parquet output
* Automated monitoring
* Automated test suite
* A single run_pipeline.sh that executes everything end-to-end

### 📘 Secondary Goal

In the notebook, two simple models (Logistic Regression + Random Forest) are trained only to validate that the ETL output is high-quality and usable for downstream ML tasks.

---

# 📦 1. Dataset Overview (Adult Income Dataset)

Based on the **UCI Adult dataset** used for income classification.

### Column Descriptions

| Column | Type | Description |
|--------|------|-------------|
| age | Numeric | Age |
| workclass | Categorical | Type of employment |
| fnlwgt | Numeric | Census weight |
| education | Categorical | Education level |
| education-num | Numeric | Education index |
| marital-status | Categorical | Marital status |
| occupation | Categorical | Job type |
| relationship | Categorical | Relationship role |
| race | Categorical | Race |
| sex | Categorical | Gender |
| capital-gain | Numeric | Investment income |
| capital-loss | Numeric | Losses |
| hours-per-week | Numeric | Weekly hours |
| native-country | Categorical | Country |
| income | Binary | >50K or ≤50K |

---

### 🧠 2. Features of this Project

- Modular ETL in `src/pipeline.py`  
- Monitoring via `src/monitor.py`  
- Pipeline visualization using **Graphviz**  
- Automated tests (pytest)  
- Reproducible environment  
- Clear separation of data, docs, src, tests  
---
### ⚙️ 3. How to Run

**Make sure you have these installed:**
```bash
pip install dask[complete] pyarrow pandas psutil pytest graphviz
brew install graphviz   # macOS
```
#### Option A — Full automation (recommended)
Run everything with two commands:
```bash
chmod +x make_pipeline.sh
./make_pipeline.sh
```
This script will:

- Run ETL pipeline
- Run monitoring script
- Save pipeline outputs
- Runs all tests
- Generates the architecture diagram
- Prints final status
#### Option B — Manual Execution
##### 1. Run the pipeline
```bash
python src/pipeline.py \
    --input src/data/adult.csv \
    --out-dir src/out_data_pipeline \
    --npartitions 4 \
    --expand-factor 5
```

##### 2. Run monitor
```bash
python src/monitor.py \
    --csv-dir src/out_data_pipeline/csv_out \
    --parquet-dir src/out_data_pipeline/parquet_out
```

##### 3. Run test suite
```bash
pytest -q
```
---
### 📓 4. Notebook Stage — Model Validation
Notebook: 02_data_pipeline.ipynb

Purpose:
- Verify Parquet loads correctly
- Explore dataset
- Train basic models to validate data quality

Models used:
- Logistic Regression
- Random Forest

These models are NOT the project’s goal. <br>
They only show that: "The ETL output is clean, structured, and suitable for downstream machine learning."


---
### 📈 5. Key Results (from Notebook)
| Model | Accuracy | ROC-AUC |
|--------|------|-------------|
| Logistic Regression | 0.76 | 0.70 |
| Random Forest | 0.98 | 0.9978 |
---
### ⚠️ 6. Limitations

Pipeline Limitations
- Jitter is intentionally mild, but still artificial → not suitable for real statistical analysis
- One-hot encoding increases dimensionality with large cardinalities
- Frequency encoding for native-country may oversimplify geographic patterns
- Expansion increases compute time proportionally to expand-factor
- Min-max scaling uses global min/max → may not generalize in streaming scenarios

Notebook Limitations
- Does not perform hyperparameter tuning
- Accuracy results depend on jitter randomness
- Conversion to full Pandas (df.compute()) will not scale to huge datasets — acceptable here because this is only validation
---