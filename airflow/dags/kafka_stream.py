from datetime import datetime
from airflow.decorators import dag, task

import json
import requests
from utils import get_kafka_producer

default_args = {
    'owner': 'pretzellz',
    'start_date': datetime(2024, 6, 11, 11, 0)
}


@dag(dag_id='kafka_ingestion',
     default_args=default_args,
     schedule_interval='* * * * *',
     catchup=False)
def kafka_ingest(email: str = 'switcharoo@yahoo.com'):
    @task
    def get_data():
        response = requests.get('https://randomuser.me/api/')
        result = response.json()['results'][0]
        return result

    @task
    def format_data(raw_data):
        return {
            'id': raw_data['login']['uuid'],
            'first_name': raw_data['name']['first'],
            'last_name': raw_data['name']['last'],
            'gender': raw_data['gender'],
            'address': f'{raw_data["location"]["street"]["number"]} {raw_data["location"]["street"]["name"]},'
                       f'{raw_data["location"]["city"]}, {raw_data["location"]["state"]}, '
                       f'{raw_data["location"]["country"]}',
            'postcode': raw_data["location"]["postcode"],
            'email': raw_data['email'],
            'username': raw_data['login']['username']
        }

    @task
    def stream_data(data):
        kafka_producer = get_kafka_producer(bootstrap_servers=['broker:29092'])
        kafka_producer.send('users_created', json.dumps(data).encode('utf-8'))


    get_data_task = get_data()
    format_data_task = format_data(get_data_task)
    stream_data_task = stream_data(format_data_task)

    get_data_task >> format_data_task >> stream_data_task


kafka_ingest_dag = kafka_ingest()
