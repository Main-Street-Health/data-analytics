import argparse
import os
import csv
import io
import joblib
import pandas as pd
import numpy as np
from sklearn.ensemble import HistGradientBoostingRegressor

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    # Hyperparameters are described here. In this simple example we are just including one hyperparameter.
#     parser.add_argument("--max_leaf_nodes", type=int, default=-1)

    # Sagemaker specific arguments. Defaults are set in the environment variables.
    parser.add_argument("--output-data-dir", type=str, default=os.environ["SM_OUTPUT_DATA_DIR"])
    parser.add_argument("--model-dir", type=str, default=os.environ["SM_MODEL_DIR"])
    parser.add_argument("--train", type=str, default=os.environ["SM_CHANNEL_TRAIN"])

    args = parser.parse_args()

    # Take the set of files and read them all into a single pandas dataframe
    input_files = [os.path.join(args.train, file) for file in os.listdir(args.train)]
    if len(input_files) == 0:
        raise ValueError(
            (
                "There are no files in {}.\n"
                + "This usually indicates that the channel ({}) was incorrectly specified,\n"
                + "the data specification in S3 was incorrectly specified or the role specified\n"
                + "does not have permission to access the data."
            ).format(args.train, "train")
        )
    raw_data = [pd.read_csv(file, header=None, engine="python") for file in input_files]
    train_data = pd.concat(raw_data)

    # labels are in the first column
    train_y = train_data.iloc[:, 0]
    train_X = train_data.iloc[:, 1:]

    # Here we support a single hyperparameter, 'max_leaf_nodes'. Note that you can add as many
    # as your training my require in the ArgumentParser above.
#     max_leaf_nodes = args.max_leaf_nodes

    # Now use scikit-learn's decision tree classifier to train the model.
    clf = HistGradientBoostingRegressor(categorical_features=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44], max_iter=1000)
    clf = clf.fit(train_X, train_y)

    # Print the coefficients of the trained classifier, and save the coefficients
    joblib.dump(clf, os.path.join(args.model_dir, "model.joblib"))


def model_fn(model_dir):
    """Deserialized and return fitted model

    Note that this should have the same name as the serialized model in the main method
    """
    clf = joblib.load(os.path.join(model_dir, "model.joblib"))
    return clf

# Deserialize the Invoke request body into an object we can perform prediction on
def input_fn(request_body, request_content_type):
    if request_content_type != "text/csv":
        raise Exception(f'Unsupported content type: {request_content_type}')

    return csv_to_numpy(request_body)


def csv_to_numpy(string_like):  # type: (str) -> np.array
    """Convert a CSV object to a numpy array.
    Pulled from https://github.com/aws/sagemaker-containers/blob/master/src/sagemaker_containers/_encoders.py

    Note (BJP): Required to override default np.astype() in order to successfully cast nulls from csv empty string.
                Also addresses issue of batch size of one by expanding the dims

    Args:
        string_like (str): CSV string.
    Returns:
        (np.array): numpy array
    """
    stream = io.StringIO(string_like)
    reader = csv.reader(stream, delimiter=",", quotechar='"', doublequote=True, strict=True)
    array = np.array([pd.to_numeric(row) for row in reader]).squeeze()

    if len(array.shape) == 1:
        array = np.expand_dims(array, axis=0)

    return array