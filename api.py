import csv
import os
import time
import traceback
from io import StringIO

import pandas as pd
from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from google.cloud import storage

from main import choose_api_key, enrich_data_with_psi_api, save_enriched_url_to_bq
from params import PSI_API_KEY, PSI_API_KEY_1, PSI_API_KEY_2, PSI_API_KEY_3

app = FastAPI()
app.state.keys = [PSI_API_KEY, PSI_API_KEY_1, PSI_API_KEY_2, PSI_API_KEY_3]
# init timer
app.state.timer = {k: time.time() for k in app.state.keys}

# Allow all requests (
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


@app.get("/")
def index():
    return {"status": "up & running"}


@app.get("/simple_query")
def query(url, return_results_only: bool = False):
    key = choose_api_key(app.state.keys)
    enriched_df = enrich_data_with_psi_api(site_url=url, api_key=key)

    if return_results_only:
        print("hello")
        return enriched_df.to_json(orient="records")

    save_enriched_url_to_bq(enriched_df=enriched_df)

    return {"status": "enriched url dumped in db"}


@app.post("/query_csv")
def querytator(csv_file: UploadFile = File(...)):
    contents = csv_file.file.read()
    buffer = StringIO(contents.decode("utf-8"))
    csvReader = csv.DictReader(buffer)

    if "url" not in csvReader.fieldnames:
        return {"error": 'a field named "url" is required'}

    logs = {"sucess": [], "error": []}

    for row in csvReader:
        url = row["url"]
        key, app.state.timer = choose_api_key(app.state.keys, use_timer=app.state.timer)
        try:
            enriched_df = enrich_data_with_psi_api(site_url=url, api_key=key)
            save_enriched_url_to_bq(enriched_df=enriched_df)
            print(f"{url} has been enriched and loaded to BQ")
            logs["sucess"].append(url)
        except:
            logs["error"].append((url, traceback.format_exc()))

    return logs


@app.get("/query_gcs")
def query_from_gcs(bucket_name, blob_name):
    if os.environ.get("ENV") == "DEV":
        client = storage.Client.from_service_account_json("gcpkey.json")
    else:
        client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    data = blob.download_to_filename("thanks.csv")
    df = pd.read_csv("thanks.csv")

    if "url" not in df.columns:
        return {"error": 'a field named "url" is required'}

    logs = {"sucess": [], "error": []}

    for url in df["url"]:
        key, app.state.timer = choose_api_key(app.state.keys, use_timer=app.state.timer)
        try:
            enriched_df = enrich_data_with_psi_api(site_url=url, api_key=key)
            save_enriched_url_to_bq(enriched_df=enriched_df)
            print(f"{url} has been enriched and loaded to BQ")
            logs["sucess"].append(url)
        except:
            logs["error"].append((url, traceback.format_exc()))

    return logs
