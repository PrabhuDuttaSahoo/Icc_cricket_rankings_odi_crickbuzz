from googleapiclient.discovery import build


def trigger_df_job(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "My First Project"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "bigquery-load",  # Provide a unique name for the job
        "parameters": {
        "javascriptTextTransformGcsPath": "gs://dataflow_metadata_pds/udf.js",
        "JSONPath": "gs://dataflow_metadata_pds/bq.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "sacred-footing-340315.cricket_dataset.icc-odi-batsman-ranking",
        "inputFilePattern": "gs://cricket_ranking_pds/batsmen_rankings.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://dataflow_metadata_pds",
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)