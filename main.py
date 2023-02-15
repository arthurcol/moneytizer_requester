import os
import time
from typing import List

import numpy as np
import pandas as pd
import requests
from colorama import Fore, Style
from google.cloud import bigquery
from numpy import NaN

from params import DATASET, PROJECT, PSI_API_KEY, TABLE, TABLE_TO


# Function to retrieve data from page speed insights API
def page_speed_insight_kpis(site, key):
    # API request
    url_api = f"https://www.googleapis.com/pagespeedonline/v5/runPagespeed?url={site}&key={key}"

    response = requests.get(url_api)
    data = response.json()

    # Handling error when calling API
    if "error" in data.keys():
        code_error = data["error"]["code"]
        code_message = data["error"]["message"]
        print(
            Fore.RED
            + f"Error found in API call {code_error}. Reason mentionned:{code_message}\n"
            + Style.RESET_ALL
        )
        lighthouse_score = NaN
        LCP = NaN
        FID = NaN
        CLS = NaN
        FCP = NaN
        INP = NaN
        TTFB = NaN

    else:
        # Get lighthouse_score available for all request
        if "lighthouseResult" in data.keys():
            lighthouse_score = data["lighthouseResult"]["categories"]["performance"][
                "score"
            ]
        else:
            lighthouse_score = NaN

        # Get additional metrics if available
        if "metrics" in data["loadingExperience"].keys():
            if (
                "LARGEST_CONTENTFUL_PAINT_MS"
                in data["loadingExperience"]["metrics"].keys()
            ):
                LCP = (
                    data["loadingExperience"]["metrics"]["LARGEST_CONTENTFUL_PAINT_MS"][
                        "percentile"
                    ]
                    / 1000
                )
            else:
                LCP = NaN

            if "FIRST_INPUT_DELAY_MS" in data["loadingExperience"]["metrics"].keys():
                FID = data["loadingExperience"]["metrics"]["FIRST_INPUT_DELAY_MS"][
                    "percentile"
                ]
            else:
                FID = NaN

            if (
                "CUMULATIVE_LAYOUT_SHIFT_SCORE"
                in data["loadingExperience"]["metrics"].keys()
            ):
                CLS = (
                    data["loadingExperience"]["metrics"][
                        "CUMULATIVE_LAYOUT_SHIFT_SCORE"
                    ]["percentile"]
                    / 100
                )
            else:
                CLS = NaN

            if (
                "FIRST_CONTENTFUL_PAINT_MS"
                in data["loadingExperience"]["metrics"].keys()
            ):
                FCP = (
                    data["loadingExperience"]["metrics"]["FIRST_CONTENTFUL_PAINT_MS"][
                        "percentile"
                    ]
                    / 1000
                )
            else:
                FCP = NaN

            if (
                "EXPERIMENTAL_INTERACTION_TO_NEXT_PAINT"
                in data["loadingExperience"]["metrics"].keys()
            ):
                INP = data["loadingExperience"]["metrics"][
                    "EXPERIMENTAL_INTERACTION_TO_NEXT_PAINT"
                ]["percentile"]
            else:
                INP = NaN

            if (
                "EXPERIMENTAL_TIME_TO_FIRST_BYTE"
                in data["loadingExperience"]["metrics"].keys()
            ):
                TTFB = (
                    data["loadingExperience"]["metrics"][
                        "EXPERIMENTAL_TIME_TO_FIRST_BYTE"
                    ]["percentile"]
                    / 1000
                )
            else:
                TTFB = NaN

        else:
            LCP = NaN
            FID = NaN
            CLS = NaN
            FCP = NaN
            INP = NaN
            TTFB = NaN

    # KPIs needed stored in a dict to create a DataFrame
    dict_kpis = {
        "site_url": [site],
        "lighthouse_score": [lighthouse_score],
        "LCP": [LCP],
        "FID": [FID],
        "CLS": [CLS],
        "FCP": [FCP],
        "INP": [INP],
        "TTFB": [TTFB],
    }

    return pd.DataFrame(dict_kpis)


# Function to retrieve data from similar web api
# Function to clean json of similar web
def last_key(my_dict):
    try:
        EstimatedMonthlyVisitsV1 = sorted(my_dict.items(), key=lambda item: item[0])[1][
            -1
        ]
        return EstimatedMonthlyVisitsV1
    except:
        EstimatedMonthlyVisitsV1 = float(NaN)
        return EstimatedMonthlyVisitsV1


# Function to get data from similar web
def get_data_from_similar(url):
    new_data = pd.DataFrame()
    headers = {
        "authority": "data.similarweb.com",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "no-cache",
        "cookie": "_vwo_uuid=J5D18666EAD94652B0761CC5BBF9936C9; _vwo_uuid_v2=D45D83C6F8108F4D129E872CE7D1C3980|c84ca154a3511c2ee2457f61b9e404e3; _fbp=fb.1.1633615155708.570983719; _wingify_pc_uuid=cfc41fbd79864cfb854f14993857b1e7; wingify_donot_track_actions=0; sw_extension_installed=1635522574358; _hjSessionUser_1406830=eyJpZCI6Ijc0ZGY2MWNhLTQ1MGYtNWFiOS1hMGFhLTVkY2Q5MzdhNTA1NyIsImNyZWF0ZWQiOjE2NDI2ODU5NDQ4ODksImV4aXN0aW5nIjp0cnVlfQ==; similarweb.contactform.id=2dc8273f-3026-4a4e-94fd-19b5cd8b8519; _BEAMER_USER_ID_zBwGJEbQ32550=3f34b93a-a488-46a6-b9c8-cf64460a09d1; _BEAMER_FIRST_VISIT_zBwGJEbQ32550=2022-03-02T10:06:13.022Z; sgID=59c86f70-9fbf-aba8-7ddd-3b7ac115b3ab; _clck=iebn4o|1|f50|0; __qca=P0-692086059-1667508480674; __zlcmid=1CllLk9IwQxpk9s; fs_cid=1.0",
        "pragma": "no-cache",
        "sec-ch-ua": 'Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "none",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": r"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    }
    r = requests.get(
        f"https://data.similarweb.com/api/v1/data?domain={url}", headers=headers
    )
    result = r.json()

    new_data["site_url"] = [url]

    if "status" in result.keys():
        new_data["Social"] = NaN
        new_data["Paid_Referrals"] = NaN
        new_data["Mail"] = NaN
        new_data["Referrals"] = NaN
        new_data["Search"] = NaN
        new_data["Direct"] = NaN
        new_data["BounceRate"] = NaN
        new_data["PagePerVisit"] = NaN
        new_data["Category"] = NaN
        new_data["EstimatedMonthlyVisits"] = NaN
        new_data["Top_Geo"] = NaN

    else:
        if result["TrafficSources"] is not None:
            # new_data["site_url"] = [url]
            new_data["Social"] = [result.get("TrafficSources").get("Social")]
            new_data["Paid_Referrals"] = result.get("TrafficSources").get("Referrals")
            new_data["Mail"] = result.get("TrafficSources").get("Mail")
            new_data["Referrals"] = result.get("TrafficSources").get("Referrals")
            new_data["Search"] = result.get("TrafficSources").get("Search")
            new_data["Direct"] = result.get("TrafficSources").get("Direct")
            new_data["BounceRate"] = result.get("Engagments").get("BounceRate")
            new_data["PagePerVisit"] = result.get("Engagments").get("PagePerVisit")
            new_data["Category"] = result.get("Category", "Unknown")
            new_data["EstimatedMonthlyVisits"] = last_key(
                result["EstimatedMonthlyVisits"]
            )

            new_data["Social"] = new_data["Social"].astype("float64")
            new_data["Paid_Referrals"] = new_data["Paid_Referrals"].astype("float64")
            new_data["Mail"] = new_data["Mail"].astype("float64")
            new_data["Referrals"] = new_data["Referrals"].astype("float64")
            new_data["Search"] = new_data["Search"].astype("float64")
            new_data["Direct"] = new_data["Direct"].astype("float64")
            new_data["BounceRate"] = new_data["BounceRate"].astype("float64")
            new_data["PagePerVisit"] = new_data["PagePerVisit"].astype("float64")
            new_data["Category"] = new_data["Category"].astype("string")
            new_data["EstimatedMonthlyVisits"] = new_data[
                "EstimatedMonthlyVisits"
            ].astype("float64")

            if len(result.get("TopCountryShares")) == 0:
                new_data["Top_Geo"] = NaN
            else:
                new_data["Top_Geo"] = result.get("TopCountryShares")[0]["Country"]
                new_data["Top_Geo"] = new_data["Top_Geo"].astype("string")

    return new_data


# Function to enrich the data, take from BigQuery and return to another BigQuery table
def enrich_data_with_psi_api(site_url, api_key):
    # request both api and merge
    psi_df = page_speed_insight_kpis(site_url, api_key)
    sw_df = get_data_from_similar(site_url)
    sw_psi_df = pd.merge(psi_df, sw_df, on="site_url", how="left")

    return sw_psi_df


def save_enriched_url_to_bq(enriched_df):
    if os.environ.get("ENV") == "DEV":
        client = bigquery.Client.from_service_account_json("gcpkey.json")
    else:
        client = bigquery.Client()
    table_to_send_to = f"{PROJECT}.{DATASET}.{TABLE_TO}"
    write_mode = "WRITE_APPEND"
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_mode,
        schema=[
            bigquery.SchemaField("site_url", "STRING"),
            bigquery.SchemaField("lighthouse_score", "FLOAT"),
            bigquery.SchemaField("LCP", "FLOAT"),
            bigquery.SchemaField("FID", "INTEGER"),
            bigquery.SchemaField("CLS", "FLOAT"),
            bigquery.SchemaField("FCP", "FLOAT"),
            bigquery.SchemaField("INP", "INTEGER"),
            bigquery.SchemaField("TTFB", "FLOAT"),
            bigquery.SchemaField("Social", "FLOAT"),
            bigquery.SchemaField("Paid_Referrals", "FLOAT"),
            bigquery.SchemaField("Mail", "FLOAT"),
            bigquery.SchemaField("Referrals", "FLOAT"),
            bigquery.SchemaField("Search", "FLOAT"),
            bigquery.SchemaField("Direct", "FLOAT"),
            bigquery.SchemaField("BounceRate", "FLOAT"),
            bigquery.SchemaField("PagePerVisit", "FLOAT"),
            bigquery.SchemaField("Category", "STRING"),
            bigquery.SchemaField("EstimatedMonthlyVisits", "INTEGER"),
            bigquery.SchemaField("Top_Geo", "STRING"),
        ],
    )

    # Send the entire df to BigQuery
    job = client.load_table_from_dataframe(
        enriched_df, table_to_send_to, job_config=job_config
    )
    job.result()
    print(Fore.GREEN + f"Loaded to BigQuery\n" + Style.RESET_ALL)


def choose_api_key(keys: List, use_timer=False):
    if not use_timer:
        return np.random.choice(keys)

    i = 0
    while True:
        i += 1
        now = time.time()
        print(Fore.CYAN + f"Selected API key with timer,round {i}" + Style.RESET_ALL)
        for k, t in use_timer.items():
            if i == 1:  # so we don't have to wait 20s first round
                use_timer[k] = now
                return k, use_timer
            if now - t > 20:
                use_timer[k] = now
                return k, use_timer
