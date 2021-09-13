import logging
import requests

import apache_beam as beam
from tenacity import retry, wait_exponential


class SectionsTransform(beam.DoFn):
    def __init__(self, known_args):
        pass

    # runs for each record in pcollection
    def process(self, text):
        output = dict()

        # iterate through BigQuery columns
        for key in [record['name'] for record in Sections.table_schema['fields']]:   

            output[key] = text.get(key)

        yield output


class Sections(beam.DoFn):

    table_schema = {
        'fields': [
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'sis_section_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'integration_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'sis_import_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'course_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'sis_course_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'start_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'end_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'restrict_enrollments_to_section_dates', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'nonxlist_course_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'total_students', 'type': 'INTEGER', 'mode': 'NULLABLE'}
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

        logging.info(f"Fetching sections for course id: {element['id']}")

        course_id = element['id']
        sections = list()

        done = False
        per_page = 100
        url = f'{self.base_url}/api/v1/courses/{course_id}/sections?page=1&per_page={per_page}' \
            + '&include=total_students'

        while not done:

            response = self.fetch_data(url)
            
            if isinstance(response.json(), list):
                logging.info(f'Retrieved  {len(response.json())} sections')
                sections = sections + response.json()
            else:
                logging.warn(f'Course id {course_id} did not return a sections list')

            if 'next' in response.links and response.links['current']['url'] != response.links['next']['url']:
                url = response.links['next']['url']
            elif 'last' in response.links and response.links['current']['url'] != response.links['last']['url']:
                url = response.links['last']['url']
            else:
                done = True

        return sections


    def teardown(self):
        pass
