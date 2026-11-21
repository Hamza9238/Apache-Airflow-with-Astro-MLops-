from airflow.decorators import dag, task
from datetime import datetime
from include import ml_pipeline

@dag(
    dag_id="weekly_model_comparison",
    start_date = datetime(2026, 1, 1),
    schedule="@weekly",
    catchup= False
)
def weekly_model_comparison():

    @task
    def preprocess_data():
        path = ml_pipeline.preprocess_data()
        return path


    @task
    def train_baseline(data_path):
        m1 = ml_pipeline.train_baseline(data_path)
        return m1

    @task
    def train_candidate(data_path):
        m2 = ml_pipeline.train_candidate(data_path)
        return m2


    @task
    def evaluate_models(baseline_path, candidate_path):
        res = ml_pipeline.evaluate_models(baseline_path, candidate_path)
        return res

    data_p = preprocess_data()
    base_m = train_baseline(data_p)
    cand_m = train_candidate(data_p)

    evaluate_models(base_m, cand_m)

weekly_model_comparison()
