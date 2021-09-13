import logging
import requests

import apache_beam as beam
from tenacity import retry, wait_exponential


class CoursesTransform(beam.DoFn):
    def __init__(self, known_args):
        pass

    # runs for each record in pcollection
    def process(self, text):
        output = dict()

        # iterate through BigQuery columns
        for key in [record['name'] for record in Courses.table_schema['fields']]:   

            output[key] = text.get(key)

        yield output


class Courses(beam.DoFn):

    table_schema = {
        'fields': [
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'account_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'uuid', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'start_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'grading_standard_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'is_public', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'course_code', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'default_view', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'root_account_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'enrollment_term_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'license', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'grade_passback_setting', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'end_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'public_syllabus', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'public_syllabus_to_auth', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'storage_quota_mb', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'is_public_to_auth_users', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'apply_assignment_group_weights', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'total_students', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {
                'name': 'calendar',
                'type': 'RECORD',
                'fields': [
                    {'name': 'ics', 'type': 'STRING', 'mode': 'NULLABLE'},
                ],
                'mode': 'NULLABLE'
            },
            {'name': 'time_zone', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'blueprint', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'hide_final_grades', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'workflow_state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'restrict_enrollments_to_course_dates', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {
                'name': 'teachers',
                'type': 'RECORD',
                'fields': [
                    {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'display_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'avatar_image_url', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'html_url', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'pronouns', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'fake_student', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}
                ],
                'mode': 'REPEATED'
            },
            {
                'name': 'term',
                'type': 'RECORD',
                'fields': [
                    {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'start_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                    {'name': 'end_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                    {'name': 'workflow_state', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'grading_period_group_id', 'type': 'INTEGER', 'mode': 'NULLABLE'}
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

        logging.info(f"Fetching courses associated with term id: {element['id']}")

        term_id = element['id']

        courses = list()

        done = False
        per_page = 100
        url = f'{self.base_url}/api/v1/accounts/1/courses?page=1&per_page={per_page}' \
            + f'&with_enrollments=true&published=true&enrollment_term_id={term_id}'

        while not done:

            response = self.fetch_data(url)

            if isinstance(response.json(), list):
                logging.info(f'Retrieved  {len(response.json())} courses')
                courses = courses + response.json()
            else:
                logging.warn(f'Request did not return a courses list')

            if 'next' in response.links and response.links['current']['url'] != response.links['next']['url']:
                url = response.links['next']['url']
            elif 'last' in response.links and response.links['current']['url'] != response.links['last']['url']:
                url = response.links['last']['url']
            else:
                done = True

        return courses


    def teardown(self):
        pass
