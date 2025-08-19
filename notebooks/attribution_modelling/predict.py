# predict.py

import os
import pickle
import json
import argparse
import pandas as pd
import time

from sqlalchemy import text
from src import cb_utils

# --- 1. Artifact & Data Loading ---
def load_artifacts(artifacts_dir):
    """Loads all necessary artifacts from the specified directory."""
    print(f"Loading artifacts from: {artifacts_dir}")
    
    # Load Model
    with open(os.path.join(artifacts_dir, "tuned_attribution_model.pkl"), 'rb') as f:
        model = pickle.load(f)
        
    # Load Feature List
    with open(os.path.join(artifacts_dir, "top_20_features.json"), 'r') as f:
        top_features = json.load(f)
        
    # Load Label Encoders
    with open(os.path.join(artifacts_dir, "label_encoders.pkl"), 'rb') as f:
        encoders = pickle.load(f)

    # Load Metadata to get categorical column names
    with open(os.path.join(artifacts_dir, "model_metadata.json"), 'r') as f:
        metadata = json.load(f)
        categorical_cols = [col for col in metadata['categorical_features'] if col in top_features]

    print("Artifacts loaded successfully.")
    return model, top_features, categorical_cols, encoders

def load_new_data():
    """Loads the raw data from the database."""
    print("Pulling query from db...")
    query = "select * from daug.attribution_probability_patients;"
    df = cb_utils.sql_query_to_df(query, use_cache=True, source='msh_member_doc')
    df = df.drop(columns=['attr_predict', 'attr_prob'], errors='ignore')  # Drop existing prediction columns if they exist
    return df

# --- 2. Preprocessing for New Data ---
def preprocess_new_data(df, top_features, categorical_cols, encoders):
    """
    Applies the EXACT same preprocessing steps to new data using the loaded encoders.
    IMPORTANT: This function does not .fit() anything, it only .transform().
    """
    print("Preprocessing new data for prediction...")
    
    # Label Encode categorical features using the LOADED encoders
    df_processed = df.copy()
    for col in categorical_cols:
        if col not in df_processed.columns:
             raise ValueError(f"Missing required categorical column in new data: {col}")
        if col in encoders:
            le = encoders[col]
            
            # Fill missing values with the same 'missing' placeholder and ensure string type
            df_processed[col] = df_processed[col].fillna('missing').astype(str)
            
            # --- FIX STARTS HERE ---
            # Handle new, unseen categories in the data robustly.
            known_classes = set(le.classes_)
            
            # Determine the fallback value. Use 'missing' if the encoder knows it,
            # otherwise use the most frequent class (the first one learned during fit).
            fallback_value = 'missing' if 'missing' in known_classes else le.classes_[0]

            df_processed[col] = df_processed[col].apply(
                lambda x: x if x in known_classes else fallback_value
            )
            # --- FIX ENDS HERE ---
            
            # Now transform
            df_processed[col] = le.transform(df_processed[col])
        else:
            print(f"Warning: No encoder found for categorical column '{col}'. Skipping encoding.")


    # Ensure all top features are present, filling missing numerical columns with 0 or a median if appropriate
    for col in top_features:
        if col not in df_processed.columns:
            print(f"Warning: Feature '{col}' not in new data. Filling with 0.")
            df_processed[col] = 0 # Or use a more sophisticated imputation strategy
            
    # Select only the features the model was trained on, in the correct order
    df_final = df_processed[top_features]
    
    print(f"Preprocessing complete. Final shape for prediction: {df_final.shape}")
    return df_final

# --- 3. Prediction ---
def make_predictions(df, model, threshold=0.5):
    """Makes predictions on preprocessed data using a specified probability threshold."""
    print(f"Making predictions with threshold: {threshold}...")
    probabilities = model.predict_proba(df)[:, 1] # Probability of the 'True' class
    predictions = (probabilities >= threshold).astype(int)
    return predictions, probabilities

def update_database_with_predictions(df):
    """Updates the database with the prediction results."""
    print("Updating database with predictions...")
    
    table_name = 'daug.attribution_probability_patients'
    prediction_col = 'attr_predict'
    probability_col = 'attr_prob'
    primary_key = 'patient_id'
    
    engine = cb_utils.get_engine(source='msh_member_doc')
    
    # For safety, let's create a temporary table to stage the updates
    temp_table_name = f"temp_update_{int(time.time())}"
    
    update_df = df[[primary_key, probability_col, prediction_col]].copy()
    
    try:
        # Write the predictions to a temporary table
        update_df.to_sql(temp_table_name, engine, index=False, if_exists='replace')

        print(update_df)
        # Construct and execute the UPDATE statement
        update_query = f"""
        UPDATE {table_name} AS target
        SET
            {prediction_col} = source.{prediction_col},
            {probability_col} = source.{probability_col}
        FROM {temp_table_name} AS source
        WHERE target.{primary_key} = source.{primary_key};
        """
        
        with engine.connect() as connection:
            connection.execute(text(update_query))
            connection.commit()
            
        print(f"Successfully updated {len(df)} records in {table_name}.")

    finally:
        print("Not Cleaning up temporary tables...")
        # Clean up the temporary table
        #with engine.connect() as connection:
        #    connection.execute(text(f"DROP TABLE IF EXISTS {temp_table_name};"))

# --- Main Orchestration ---
def main(artifacts_dir, threshold):
    start_time = time.time()
    
    # 1. Load artifacts
    model, top_features, categorical_cols, encoders = load_artifacts(artifacts_dir)
    
    # 2. Load and preprocess new data
    new_df = load_new_data()
    X_new_processed = preprocess_new_data(new_df, top_features, categorical_cols, encoders)
    
    # 3. Make predictions
    predictions, probabilities = make_predictions(X_new_processed, model, threshold)
    
    # 4. Create results DataFrame
    results_df = new_df.copy()
    results_df['attr_predict'] = predictions
    results_df['attr_prob'] = probabilities
    #print(results_df[['patient_id', 'attr_predict', 'attr_prob']].head())
    
    # 5. Update database
    #first_cols = ['patient_id', 'predicted_attribution', 'predicted_probability']
    #other_cols = [col for col in results_df.columns if col not in first_cols]
    #results_df = results_df[first_cols + other_cols]

    # Save to CSV
    #results_df.to_csv(output_path, index=False)

    update_database_with_predictions(results_df)
    
    end_time = time.time()
    print(f"\nPrediction pipeline finished in {end_time - start_time:.2f} seconds.")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Predict patient attribution using a trained model.")
    parser.add_argument('--artifacts_dir', type=str, required=True, help='Directory containing the saved model artifacts.')
    parser.add_argument(
        '--threshold',
        type=float,
        default=0.5,
        help='Probability threshold for classification (default: 0.5).'
    )
    
    args = parser.parse_args()
    
    main(args.artifacts_dir, args.threshold)