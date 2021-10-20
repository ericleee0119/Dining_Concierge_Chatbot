import pandas as pd
import csv
import boto3
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


def main():
    host = 'search-domainyelprest-opbbigm37ekeuhwnf6vezd25ua.us-west-2.es.amazonaws.com'
    region = 'us-west-2'

    service = 'es'
    credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        region,
        service,
        session_token=credentials.token)

    es = Elasticsearch(
        hosts = [{'host': host, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )

    csv_file = pd.read_csv("Restaurants.csv")
    rest_id = csv_file["uuid"].tolist()
    cuisine = csv_file["cuisine"].tolist()

    for i, n in enumerate(rest_id):
        index_data = {
            'id': n,
            'categories': cuisine[i]
        }
        print(index_data)

        es.index(
            index="restaurants",
            doc_type="Restaurant",
            id=n,
            body=index_data,
            refresh=True)

    print("Upload Finished!")

if __name__ == "__main__":
    main()
