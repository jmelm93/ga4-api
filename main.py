import os
import pandas as pd
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google_auth_oauthlib.flow import InstalledAppFlow

load_dotenv()

## START: CONFIG - CHANGE THESE INPUTS ##

PROPERTY_ID = os.getenv('PROPERTY_ID') ## update this to a string id (e.g., '1234567') or use a .env file if you want to publish this code to GitHub/elsewhere
DIMENSION_LIST = ['landingPage', 'sessionDefaultChannelGroup'] # https://ga-dev-tools.google/ga4/dimensions-metrics-explorer/
METRIC_LIST = ['sessions'] # https://ga-dev-tools.google/ga4/dimensions-metrics-explorer/
START_DATE = '2023-01-01'
END_DATE = '2023-05-01'

## END: CONFIG ##


SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
KEY_FILE_LOCATION = os.path.join(os.path.dirname(__file__), "./credentials/client_secret.json")


JOB_OBJECT = {
    'property_id': PROPERTY_ID,
    'dimension_name_list': DIMENSION_LIST,
    'metric_name_list': METRIC_LIST,
    'start_date': START_DATE,
    'end_date': END_DATE
}


def get_credentials():
    creds = None
    if os.path.exists('./credentials/token.json'):
        creds = Credentials.from_authorized_user_file('./credentials/token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(KEY_FILE_LOCATION, SCOPES)
            creds = flow.run_local_server(port=0)
        with open('./credentials/token.json', 'w') as token:
            token.write(creds.to_json())
    return creds


def get_report(job_object, credentials):
    client = BetaAnalyticsDataClient(credentials=credentials)

    request = RunReportRequest(
        property=f"properties/{job_object['property_id']}",
        dimensions=[
            Dimension(name=dimension_name) for dimension_name in job_object['dimension_name_list']
        ],
        metrics=[
            Metric(name=metric_name) for metric_name in job_object['metric_name_list']
        ],
        date_ranges=[
            DateRange(start_date=job_object['start_date'], end_date=job_object['end_date'])
        ],
    )

    response = client.run_report(request)

    return response


def transform_response(response):
    data = []

    for row in response.rows:
        dimensions = [dim.value for dim in row.dimension_values]
        metrics = [metric.value for metric in row.metric_values]
        data.append(dimensions + metrics)

    dimension_names = [header.name for header in response.dimension_headers]
    metric_names = [header.name for header in response.metric_headers]

    columns = dimension_names + metric_names

    df = pd.DataFrame(data, columns=columns)

    return df


def main(job_object):
    credentials = get_credentials()
    response = get_report(job_object, credentials)
    df = transform_response(response)
    print(df)    


if __name__ == '__main__':
    main(job_object=JOB_OBJECT)
