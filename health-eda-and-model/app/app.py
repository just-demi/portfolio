import streamlit as st
import pandas as pd
import joblib
import shap
import numpy as np
import os
import matplotlib.pyplot as plt


# ---------------------------
# Paths
# ---------------------------
# Get the absolute path of the directory where app.py is located
APP_DIR = os.path.dirname(os.path.abspath(__file__))

# Get the Project Root (go up one level from app/)
PROJECT_ROOT = os.path.dirname(APP_DIR)

# Construct paths to the models
MODEL_PATH = os.path.join(PROJECT_ROOT, "notebooks", "saved_models", "Logistic_Regression.joblib")
SCALER_PATH = os.path.join(PROJECT_ROOT, "notebooks", "saved_models", "scaler.joblib")
FEATURE_ORDER_PATH = os.path.join(PROJECT_ROOT, "notebooks", "saved_models", "feature_order.joblib")

# ---------------------------
# Load model, scaler, and feature order
# ---------------------------
try:
    model = joblib.load(MODEL_PATH)
    scaler = joblib.load(SCALER_PATH)
    feature_order = joblib.load(FEATURE_ORDER_PATH)
except FileNotFoundError as e:
    st.error(f"Error loading files: {e}. Please check your folder structure.")
    st.stop()

# ---------------------------
# Feature mappings for user-friendly input
# ---------------------------
gender_map = {"Male": 1, "Female": 0}
exang_map = {"Yes": 1, "No": 0}
fbs_map = {">120 mg/dl": 1, "<=120 mg/dl": 0}
restecg_map = {"Normal": 0, "ST-T wave abnormality": 1, "Left ventricular hypertrophy": 2}
cp_map = {
    "Typical angina": 0,
    "Atypical angina": 1,
    "Non-anginal pain": 2,
    "Asymptomatic": 3
}
slope_map = {"Upsloping": 0, "Flat": 1, "Downsloping": 2}
thal_map = {"unknown": 0, "Fixed defect": 1, "Normal": 2, "Reversible defect": 3}
ca_map = {"0": 0, "1": 1, "2": 2, "3": 3}

# ---------------------------
# Numeric features
# ---------------------------
numeric_features = ['age', 'trestbps', 'chol', 'thalach', 'oldpeak']

# ---------------------------
# Streamlit UI
# ---------------------------
st.title("Heart Disease Prediction")
st.sidebar.header("Enter Patient Data:")

user_input = {
    'age': st.sidebar.slider("Age", 20, 100, 70),
    'sex': st.sidebar.selectbox("Gender", options=["Male", "Female"], index=0),
    'cp': st.sidebar.selectbox("Chest Pain Type", options=list(cp_map.keys()), index=3),
    'trestbps': st.sidebar.slider("Resting Blood Pressure", 80, 200, 180),
    'chol': st.sidebar.slider("Serum Cholesterol (mg/dl)", 100, 600, 300),
    'fbs': st.sidebar.selectbox("Fasting Blood Sugar", options=list(fbs_map.keys()), index=0),
    'restecg': st.sidebar.selectbox("Resting ECG", options=list(restecg_map.keys()), index=1),
    'thalach': st.sidebar.slider("Max Heart Rate Achieved", 70, 220, 100),
    'exang': st.sidebar.selectbox("Exercise Induced Angina", options=list(exang_map.keys()), index=0),
    'oldpeak': st.sidebar.slider("ST Depression Induced by Exercise", 0.0, 6.0, 4.0, step=0.1),
    'slope': st.sidebar.selectbox("Slope of ST Segment", options=list(slope_map.keys()), index=2),
    'ca': st.sidebar.selectbox("Number of Major Vessels (0-3)", options=list(ca_map.keys()), index=3),
    'thal': st.sidebar.selectbox("Thalassemia", options=list(thal_map.keys()), index=2)
}

# ---------------------------
# Map to numeric
# ---------------------------
input_df = pd.DataFrame([user_input])
input_df['sex'] = input_df['sex'].map(gender_map)
input_df['exang'] = input_df['exang'].map(exang_map)
input_df['fbs'] = input_df['fbs'].map(fbs_map)
input_df['restecg'] = input_df['restecg'].map(restecg_map)
input_df['cp'] = input_df['cp'].map(cp_map)
input_df['slope'] = input_df['slope'].map(slope_map)
input_df['thal'] = input_df['thal'].map(thal_map)
input_df['ca'] = input_df['ca'].map(ca_map)

# ---------------------------
# Scale numeric features
# ---------------------------
# Copy prevents setting with copy warning
input_df_processed = input_df.copy()
input_df_processed[numeric_features] = scaler.transform(input_df_processed[numeric_features])

# ---------------------------
# Ensure correct column order
# ---------------------------
input_df_processed = input_df_processed[feature_order]

# ---------------------------
# Prediction
# ---------------------------
if st.button("Predict"):
    prediction = model.predict(input_df_processed)[0]
    probabilities = model.predict_proba(input_df_processed)[0]

    # FIX: Inverted logic based on observed behavior
    # Original: 1 = Disease, 0 = No Disease
    # Observed: 1 = No Disease (Healthy), 0 = Disease
    
    if prediction == 1:
        result = "No Heart Disease"
        prob_disease = probabilities[0]     # Probability of class 0
        prob_no_disease = probabilities[1]  # Probability of class 1
    else:
        result = "Heart Disease"
        prob_disease = probabilities[0]     # Probability of class 0
        prob_no_disease = probabilities[1]  # Probability of class 1

    # Display results
    if result == "Heart Disease":
        st.error(f"Prediction: **{result}**")
    else:
        st.success(f"Prediction: **{result}**")
        
    st.write(f"Probability of Heart Disease: {prob_disease:.2f}")
    st.write(f"Probability of No Heart Disease: {prob_no_disease:.2f}")

        # ---------------------------
    # EDA + Explainability Images
    # ---------------------------
    st.subheader("Model Explainability & EDA")

    # Helper to load images
    from PIL import Image

    def load_image(name):
        return Image.open(os.path.join(PROJECT_ROOT,"app","assets", name))

    col1, col2 = st.columns(2)

    with col1:
        st.caption("Target Distribution")
        st.image(load_image("target_distribution.png"), use_column_width=True)

    with col2:
        st.caption("Logitic Regression Feature Importance Summary")
        st.image(load_image("shap.png"), use_column_width=True)

    col3, col4 = st.columns(2)

    with col3:
        st.caption("Feature Box Plot")
        st.image(load_image("box.png"), use_column_width=True)

    with col4:
        st.caption("Feature Histogram")
        st.image(load_image("hist.png"), use_column_width=True)