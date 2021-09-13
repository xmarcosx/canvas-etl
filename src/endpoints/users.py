import logging
import requests

import apache_beam as beam
from tenacity import retry, wait_exponential


class UsersTransform(beam.DoFn):
    def __init__(self, known_args):
        pass

    # runs for each record in pcollection
    def process(self, text):
        output = dict()

        # iterate through BigQuery columns
        for key in [record['name'] for record in Users.table_schema['fields']]:   

            output[key] = text.get(key)

        yield output


class Users(beam.DoFn):

    table_schema = {
        'fields': [
            {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'sortable_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'short_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'avatar_url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'locale', 'type': 'STRING', 'mode': 'NULLABLE'},
            {
                'name': 'permissions',
                'type': 'RECORD',
                'fields': [
                    {'name': 'can_update_name', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                    {'name': 'can_update_avatar', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
                    {'name': 'limit_parent_app_web_access', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}
                ],
                'mode': 'NULLABLE'
            }
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

        logging.info(f"Fetching profile information for user id: {element['user_id']}")

        user_id = element['user_id']
        url = f'{self.base_url}/api/v1/users/{user_id}?include=email'

        response = self.fetch_data(url)
        
        return [response.json()]

    def teardown(self):
        pass
