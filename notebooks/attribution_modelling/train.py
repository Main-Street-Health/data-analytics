# train.py

import os
import time
import pickle
import json
import argparse
from datetime import datetime

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, RandomizedSearchCV, StratifiedKFold
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import roc_auc_score, accuracy_score, classification_report
from sklearn.inspection import permutation_importance

# Assumes cb_utils.py is in a 'src' folder
from src import cb_utils

# --- 1. Setup & Initialization ---
def setup_output_directory(base_dir='../data'):
    """Creates a timestamped directory for storing run artifacts."""
    output_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = os.path.join(base_dir, f"{output_timestamp}_attribution_model")
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory created: {output_dir}")
    return output_dir

# --- 2. Data Loading & Preprocessing ---
def load_data():
    """Loads the raw data from the database."""
    print("Pulling query from db...")
    query = "SELECT * FROM attribution_prediction_model;"
    df = cb_utils.sql_query_to_df(query, use_cache=True, source='msh_member_doc')
    return df

def preprocess_data(df):
    """Performs all preprocessing steps on the raw dataframe."""
    print("Preprocessing data...")
    # Filter for 'ma' contract type
    df = df.loc[df['contract_type_key'] == 'ma'].copy()
    
    # Define target and features
    target_col = 'is_attributed_in_prediction_year'
    id_cols_to_drop = ['patient_id', 'primary_rpo_id', 'primary_referring_partner_id']
    other_cols_to_drop = ['attributed_in_year_n'] #, 'contract_type_key', 'current_primary_rpl_type', 'appt_count_group']
    
    features_df = df.drop(columns=[target_col] + id_cols_to_drop + other_cols_to_drop, axis=1, errors='ignore')
    target = df[target_col]
    
    # Identify categorical columns
    categorical_cols = features_df.select_dtypes(include=['object', 'bool']).columns.tolist()
    id_cols = [col for col in features_df.columns if col.endswith('_id')]
    categorical_cols.extend(id_cols)
    categorical_cols = list(dict.fromkeys(categorical_cols)) # Remove duplicates
    numerical_cols = [col for col in features_df.columns if col not in categorical_cols]

    # Label Encode categorical features for HistGradientBoostingClassifier
    features_processed = features_df.copy()
    label_encoders = {}

    print("Label Encoding categorical features...")
    for col in categorical_cols:
        le = LabelEncoder()
        # Fill missing values with a placeholder string before encoding
        features_processed[col] = features_processed[col].fillna('missing')
        features_processed[col] = le.fit_transform(features_processed[col])
        label_encoders[col] = le
        
    print(f"Preprocessing complete. Final feature shape: {features_processed.shape}")
    return features_processed, target, categorical_cols, label_encoders, df['patient_id']

# --- 3. Model Training & Tuning ---
def train_and_tune_model(X_train, y_train, all_features_list, categorical_cols):
    """Trains a base model, finds top features, and tunes hyperparameters."""
    print("Training initial model to find feature importances...")
    
    # Get categorical feature indices for the full dataset
    categorical_feature_indices_full = [all_features_list.get_loc(col) for col in categorical_cols]

    # Initial model to get feature importance
    initial_model = HistGradientBoostingClassifier(
        random_state=42,
        categorical_features=categorical_feature_indices_full
    )
    initial_model.fit(X_train, y_train)

    # Permutation Importance
    print("Calculating permutation feature importance...")
    perm_importance = permutation_importance(
        initial_model, 
        X_train, 
        y_train, 
        n_repeats=10, 
        random_state=42, 
        scoring='roc_auc', 
        n_jobs=-1
    )

    feature_importance_df = pd.DataFrame({
        'feature': X_train.columns,
        'importance_mean': perm_importance.importances_mean,
        'importance_std': perm_importance.importances_std
    }).sort_values('importance_mean', ascending=False)
    
    top_20_features = feature_importance_df.head(20)['feature'].tolist()
    print(f"Top 20 features identified: {top_20_features}")

    # Prepare for tuning with top 20 features
    X_train_top20 = X_train[top_20_features]
    categorical_features_top20_indices = [X_train_top20.columns.get_loc(col) for col in top_20_features if col in categorical_cols]
    
    # Hyperparameter search space
    param_distributions = {
        'max_iter': [100, 200, 300],  # Number of boosting iterations
        'learning_rate': [0.05, 0.1, 0.15, 0.2],  # Step size shrinkage
        'max_depth': [3, 6, 10, None],  # Maximum depth of trees
        'min_samples_leaf': [20, 50, 100],  # Minimum samples in leaf node
        'max_bins': [100, 200, 255],  # Maximum number of bins for numerical features
        'l2_regularization': [0.0, 0.1, 0.5, 1.0],  # L2 regularization parameter
        'early_stopping': [True],  # Enable early stopping
        'validation_fraction': [0.1],  # Fraction of data to use for validation
        'n_iter_no_change': [10],  # Number of iterations without improvement before stopping
        'tol': [1e-7]  # Tolerance for early stopping
    }
    
    base_estimator = HistGradientBoostingClassifier(
        random_state=42,
        categorical_features=categorical_features_top20_indices,
        verbose=0,
        #early_stopping=True,
        #validation_fraction=0.1,
        #n_iter_no_change=10,
    )
    
    cv = StratifiedKFold(n_splits=3, shuffle=True, random_state=42)
    
    print("Starting hyperparameter search with RandomizedSearchCV...")
    
    random_search = RandomizedSearchCV(
        estimator=base_estimator,
        param_distributions=param_distributions,
        n_iter=50,
        cv=cv,
        scoring='roc_auc',
        n_jobs=-1,
        verbose=1,
        random_state=42
    )
    
    random_search.fit(X_train_top20, y_train)
    
    print(f"Best cross-validation ROC AUC: {random_search.best_score_:.4f}")
    print(f"Best parameters: {random_search.best_params_}")
    
    return random_search.best_estimator_, top_20_features

# --- 4. Evaluation & Artifact Saving ---
def evaluate_model(model, X_test, y_test, top_features, threshold=0.5):
    """Evaluates the final model on the test set using a specified probability threshold."""
    print(f"\nEvaluating final tuned model on test set with threshold: {threshold}...")
    X_test_top = X_test[top_features]
    y_proba = model.predict_proba(X_test_top)[:, 1]
    y_pred = (y_proba >= threshold).astype(int) # Apply custom threshold
    
    roc_auc = roc_auc_score(y_test, y_proba)
    accuracy = accuracy_score(y_test, y_pred)
    
    print(f"Test Set ROC AUC: {roc_auc:.4f}")
    print(f"Test Set Accuracy (at threshold {threshold}): {accuracy:.4f}")
    print(f"\nClassification Report (Test Set at threshold {threshold}):")
    print(classification_report(y_test, y_pred))
    
    return {
        'test_roc_auc': roc_auc,
        'test_accuracy': accuracy,
        'evaluation_threshold': threshold,
        'best_params': model.get_params()
    }

def save_artifacts(output_dir, model, top_features, encoders, metadata):
    """Saves model, feature list, encoders, and metadata."""
    print("Saving model artifacts...")
    
    # Save Model
    with open(os.path.join(output_dir, "tuned_attribution_model.pkl"), 'wb') as f:
        pickle.dump(model, f)
        
    # Save Feature List
    with open(os.path.join(output_dir, "top_20_features.json"), 'w') as f:
        json.dump(top_features, f, indent=2)
        
    # Save Label Encoders
    with open(os.path.join(output_dir, "label_encoders.pkl"), 'wb') as f:
        pickle.dump(encoders, f)
        
    # Save Metadata
    with open(os.path.join(output_dir, "model_metadata.json"), 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
        
    print(f"Artifacts saved successfully to {output_dir}")

# --- Main Orchestration ---
def main(output_base_dir, threshold):
    start_time = time.time()
    
    # 1. Setup
    output_dir = setup_output_directory(output_base_dir)
    
    # 2. Load and Preprocess
    raw_df = load_data()
    X, y, categorical_cols, encoders, patient_ids = preprocess_data(raw_df)
    
    # 3. Split Data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    # 4. Train, Tune, and get final model and features
    final_model, top_features = train_and_tune_model(X_train, y_train, X.columns, categorical_cols)
    
    # 5. Evaluate
    eval_metrics = evaluate_model(final_model, X_test, y_test, top_features, threshold)

    # 6. Save Artifacts
    metadata = {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'model_type': 'HistGradientBoostingClassifier',
        'feature_count': len(top_features),
        'top_20_features': top_features,
        'categorical_features': categorical_cols, # <-- FIX: Added this line
        'training_time_seconds': time.time() - start_time,
        **eval_metrics
    }
    save_artifacts(output_dir, final_model, top_features, encoders, metadata)
    
    end_time = time.time()
    print(f"\nTraining pipeline finished in {(end_time - start_time) / 60:.2f} minutes.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Train the patient attribution model.")
    parser.add_argument(
        '--output_dir', 
        type=str, 
        default='../data', 
        help='The base directory to save model artifacts.'
    )
    parser.add_argument(
       '--threshold',
       type=float,
       default=0.5,
       help='Probability threshold for classification (default: 0.5).'
   )
    args = parser.parse_args()
    
    main(args.output_dir, args.threshold)