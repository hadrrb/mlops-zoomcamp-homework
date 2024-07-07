import mlflow
import pickle
from mlflow.tracking import MlflowClient

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

def dump_pickle(obj, filename: str):
    with open(filename, "wb") as f_out:
        return pickle.dump(obj, f_out)

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    # Specify your data exporting logic here
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_experiment(kwargs['EXPERIMENT_NAME'])

    dv, lr = data

    dump_pickle(dv, "dv.pkl")
    with mlflow.start_run():
        mlflow.sklearn.log_model(lr, "model")
        mlflow.log_artifact("dv.pkl", "transformer")

