from flask import Flask, request, render_template
import re
import os
import pandas as pd
from datetime import date
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from init import refresh_data_files_from_s3

app = Flask(__name__)

class Config:
    _metric_files_dir = os.path.join(os.getcwd(), 'data_files')

    @staticmethod
    def gather_files() -> List[str]:
        """
        gather all metric files names in the data_files directory and return as a list
        """
        return [os.path.join(Config._metric_files_dir, file_name) for file_name in os.listdir(Config._metric_files_dir)]
    
    @staticmethod
    def create_metric_file_mapping(files: List[str]) -> Dict[str, str]:
        """
        map out all the metric files with the metric they represent and return as a dict
        """
        mapping = dict()
        for file in files:
            mapping[re.search(r"^(\w+)_", file.split(os.sep)[-1]).group(1)] = file
        return mapping

class MetricDataFrame:
    metric_files_dir = Config._metric_files_dir

    @staticmethod
    def _get_metric_data(mapping: Dict[str, str], metric: str) -> pd.DataFrame:
        """
        create a pandas dataframe for a given metric from the mapping dict
        """
        try:
            metric_df = (
                pd.read_parquet(mapping[metric])
            )
            metric_df['metric'] = metric
            return metric_df
        except Exception as e:
            print(f"Error loading metric data for {metric}: {e}")

    @staticmethod
    def get_all_metrics_df(mapping: dict[str, str]) -> pd.DataFrame:
        """
        create a consolidated pandas dataframe for all metrics from the mapping dict
        """
        all_metrics_df = pd.DataFrame(columns=['entity', 'metric', 'date', 'value'])

        convert_to_date_col = lambda col: 'date' if col in ['date', 'month', 'week'] else col

        with ThreadPoolExecutor(max_workers=len(mapping)) as e:
            futures = list()
            for k in mapping.keys():
                futures.append(e.submit(MetricDataFrame._get_metric_data, mapping, metric= k))
        for future in as_completed(futures):
            metric_df = future.result()
            metric_df.columns = list(map(convert_to_date_col, metric_df.columns))
            metric_df['value'] = metric_df[[col for col in metric_df.columns if col not in ['entity', 'date', 'metric']]]
            metric_df = metric_df[['entity', 'metric', 'date', 'value']]
            all_metrics_df = pd.concat([all_metrics_df, metric_df], ignore_index=True)
        return all_metrics_df


def get_json_from_data(
    df: pd.DataFrame,
    entities: List[str] | str = None,
    metric: str = None,
    start_date: str = None,
    end_date: str = None,
):
    """Returns a json string of the dataframe with the specified entities and date range."""

    date_error_msg = {"status": "error", "message": "date must be provided as a string in ISO format (YYYY-MM-DD)."}
    entity_error_msg = {"status": "error", "message": "one or more entities provided do not exist."}
    metric_error_msg = {"status": "error", "message": "metric provided do not exist."}

    #entities and metric are required parameters
    if entities is None:
        return entity_error_msg
    if metric is None:
        return metric_error_msg
    
    df_copy = df.copy()

    if isinstance(entities, str):
        entities = [entities]
    all_entities = df_copy["entity"].unique().tolist()
    for ent in entities:
        if ent not in all_entities:
            return entity_error_msg

    if isinstance(metric, str):
        metric = [metric]
    all_metrics = df_copy["metric"].unique().tolist()
    for met in metric:
        if met not in all_metrics:
            return metric_error_msg
             
    if isinstance(start_date, str):
        try:
            start_date = date.fromisoformat(start_date)
        except Exception:
            return date_error_msg
        
    if isinstance(end_date, str):
        try:
            end_date = date.fromisoformat(end_date)
        except Exception:
            return date_error_msg

    entity_filter = df_copy["entity"].isin(entities) 
    metric_filter = df_copy["metric"].isin(metric)
    date_col = (
        "date"
        if "date" in df_copy.columns
        else ("month" if "month" in df_copy.columns else "week")
    )
    start_date_filter = (df_copy[date_col] >= start_date) if start_date else True
    end_date_filter = (df_copy[date_col] <= end_date) if end_date else True

    df_copy = df_copy[(entity_filter & start_date_filter & end_date_filter & metric_filter)]
    
    # Format date column as plain date string
    df_copy[date_col] = df_copy[date_col].apply(lambda x: x.strftime("%Y-%m-%d"))
    return df_copy.to_json(orient="records", indent=4, date_format="iso")
    

@app.route('/metrics', methods=['GET'])
def metrics():
    """API endpoint to get all available metrics."""
    metrics = {
        "metrics": list()
    }
    for k in Config.create_metric_file_mapping(Config.gather_files()).keys():
        metrics["metrics"].append({
            "name": k
            # "description": f"Description for {k} metric."
        })
    return metrics


@app.route('/metrics/<string:metric_name>', methods=['GET'])
def get_metric_data(metric_name):
    """API endpoint to get metric data in JSON format based on query parameters."""
    entities = request.args.get('entity', None)
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)

    mapping = Config.create_metric_file_mapping(Config.gather_files())
    all_metrics_df = MetricDataFrame.get_all_metrics_df(mapping)
    json_data = get_json_from_data(
        all_metrics_df,
        entities=entities.split(',') if entities else None,
        metric=metric_name,
        start_date=start_date,
        end_date=end_date
    )
    return json_data

@app.route('/')
def home():
    """
    Available endpoints:
    1. GET /metrics 
        reponse: List of all available metrics.
    2. GET /metrics/<metric_name> with the below query params
    {
    "entity": "entity_name" (required, comma-separated for multiple),
    "start_date": "YYYY-MM-DD" (optional),
    "end_date": "YYYY-MM-DD" (optional)
    } 
        response: Get metric data in JSON format.
    """    
    return render_template('index.html')

@app.route('/refresh-metrics', methods=['POST'])
def refresh():
    """Endpoint to refresh data files from S3."""
    import shutil
    shutil.rmtree(Config._metric_files_dir)
    refresh_data_files_from_s3()
    return {"status": "success", "message": "Data files refreshed from S3."}

if __name__ == '__main__':
    app.run(host="127.0.0.1", debug=True)
 