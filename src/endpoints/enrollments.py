import logging
import requests

import apache_beam as beam


class EnrollmentsTransform(beam.DoFn):
    def __init__(self, known_args):
        pass

    # runs for each record in pcollection
    def process(self, text):
        output = dict()

        # iterate through BigQuery columns
        for key in [record['name'] for record in Enrollments.table_schema['fields']]:   

            output[key] = text.get(key)

        yield output


class Enrollments(beam.DoFn):

    table_schema = {
        'fields': [
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'user_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'course_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'updated_at', 'type': 'TIMESTAMP', 'mode': 'REQUIRED'},
            {'name': 'associated_user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'start_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'end_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'course_section_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'root_account_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'limit_privileges_to_course_section', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'enrollment_state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'role', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'role_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'last_activity_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'last_attended_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'total_activity_time', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'html_url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {
                'name': 'user',
                'type': 'RECORD',
                'fields': [
                    {'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
                    {'name': 'sortable_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'short_name', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'sis_user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'integration_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'sis_import_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
                    {'name': 'login_id', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'pronouns', 'type': 'STRING', 'mode': 'NULLABLE'}
                ],
                'mode': 'NULLABLE'
            },
            {
                'name': 'grades',
                'type': 'RECORD',
                'fields': [
                    {'name': 'html_url', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'current_points', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                    {'name': 'current_grade', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'current_score', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                    {'name': 'final_grade', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'final_score', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                    {'name': 'unposted_current_score', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                    {'name': 'unposted_current_grade', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'unposted_final_score', 'type': 'FLOAT', 'mode': 'NULLABLE'},
                    {'name': 'unposted_final_grade', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'unposted_current_points', 'type': 'FLOAT', 'mode': 'NULLABLE'}
                ],
                'mode': 'NULLABLE'
            },
        ]
    }

    def __init__(self, base_url=None, endpoint=None, token=None):
        self.base_url = base_url
        self.endpoint = endpoint
        self.token = token

    def setup(self):
        pass

    def process(self, element):

        logging.info(f"Fetching enrollments for course id: {element['id']}")

        course_id = element['id']
        enrollments = list()

        done = False
        per_page = 100
        url = f'{self.base_url}/api/v1/courses/{course_id}/enrollments?page=1&per_page={per_page}' \
            + '&include=current_points'

        while not done:

            response = requests.get(
                url,
                headers={'Authorization' : f'Bearer {self.token}'})
            
            if isinstance(response.json(), list):
                logging.info(f'Retrieved  {len(response.json())} enrollments')
                enrollments = enrollments + response.json()
            else:
                logging.warn(f'Course id {course_id} did not return a enrollments list')

            if 'next' in response.links and response.links['current']['url'] != response.links['next']['url']:
                url = response.links['next']['url']
            elif 'last' in response.links and response.links['current']['url'] != response.links['last']['url']:
                url = response.links['last']['url']
            else:
                done = True

        return enrollments


    def teardown(self):
        pass
