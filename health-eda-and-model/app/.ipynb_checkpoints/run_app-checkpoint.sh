#!/bin/bash
# -----------------------------
# run_app.sh
# -----------------------------
# Reproducible setup for Heart Disease Streamlit app
# macOS compatible using conda

ENV_NAME="heart_model"

# Check if conda exists
if ! command -v conda &> /dev/null
then
    echo "conda could not be found. Please install Anaconda or Miniconda."
    exit 1
fi

# Create environment if it doesn't exist
if ! conda info --envs | grep -q "$ENV_NAME"; then
    echo "Creating conda environment '$ENV_NAME'..."
    conda create -y -n $ENV_NAME python=3.11 \
        pandas=1.5.3 numpy=1.24.3 scikit-learn=1.2.2 joblib=1.2.0 shap=0.47.2 matplotlib=3.7.2 streamlit=1.29.0
else
    echo "Environment '$ENV_NAME' already exists."
fi

# Activate environment
echo "Activating environment '$ENV_NAME'..."
source $(conda info --base)/etc/profile.d/conda.sh
conda activate $ENV_NAME

# Run the Streamlit app
echo "Running Streamlit app..."
streamlit run app.py