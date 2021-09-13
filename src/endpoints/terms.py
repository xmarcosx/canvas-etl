import logging
import requests

import apache_beam as beam
from tenacity import retry, wait_exponential


class TermsTransform(beam.DoFn):
    def __init__(self, known_args):
        pass

    # runs for each record in pcollection
    def process(self, text):
        output = dict()

        # iterate through BigQuery columns
        for key in [record['name'] for record in Terms.table_schema['fields']]:   

            output[key] = text.get(key)

        yield output


class Terms(beam.DoFn):

    table_schema = {
        'fields': [
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'start_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'end_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'workflow_state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'grading_period_group_id', 'type': 'INTEGER', 'mode': 'NULLABLE'}
        ]
    }

    def __init__(self, base_url=None, endpoint=None, token=None):
        self.base_url = base_url
        self.endpoint = endpoint
        self.token = token

    def setup(self):
        pass


    @retry(wait=wait_exponential(multiplier=1, min=4, max=10))
    def fetch_data(self, url):

        response = requests.get(url, headers={'Authorization' : f'Bearer {self.token}'})

        if response.status_code != 200:
            raise Exception
        else:
            return response    


    def process(self, element):

        terms = list()

        done = False
        per_page = 100
        url = f'{self.base_url}/api/v1/accounts/1/terms?page=1&per_page={per_page}'

        while not done:

            response = self.fetch_data(url)

            if isinstance(response.json(), list):
                terms = terms + response.json()['enrollment_terms']
            else:
                logging.warn(f'Request did not return a list')

            if 'next' in response.links and response.links['current']['url'] != response.links['next']['url']:
                url = response.links['next']['url']
            elif 'last' in response.links and response.links['current']['url'] != response.links['last']['url']:
                url = response.links['last']['url']
            else:
                done = True

        return terms


    def teardown(self):
        pass
