import logging
import requests

import apache_beam as beam
from tenacity import retry, wait_exponential


class SubmissionsTransform(beam.DoFn):
    def __init__(self, known_args):
        pass

    # runs for each record in pcollection
    def process(self, text):
        output = dict()

        # iterate through BigQuery columns
        for key in [record['name'] for record in Submissions.table_schema['fields']]:   

            output[key] = text.get(key)

        yield output


class Submissions(beam.DoFn):

    table_schema = {
        'fields': [
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'body', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'score', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'grade', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'submitted_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'assignment_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'submission_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'workflow_state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'grade_matches_current_submission', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'graded_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'grader_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'attempt', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'cached_due_date', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'excused', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'late_policy_status', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'points_deducted', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'grading_period_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'extra_attempts', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'posted_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'late', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'missing', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'seconds_late', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'preview_url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'anonymous_id', 'type': 'STRING', 'mode': 'NULLABLE'}
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

        logging.info(f"Fetching submissions for assignment id: {element['id']}")

        assignment_id = element['id']
        course_id = element['course_id']
        submissions = list()

        done = False
        per_page = 100
        url = f'{self.base_url}/api/v1/courses/{course_id}/assignments/{assignment_id}/submissions' \
            + f'?page=1&per_page={per_page}'

        while not done:

            response = self.fetch_data(url)
            
            if isinstance(response.json(), list):
                logging.info(f'Retrieved  {len(response.json())} submissions')
                submissions = submissions + response.json()
            else:
                logging.warn(f'Course id {course_id} did not return a submissions list')

            if 'next' in response.links and response.links['current']['url'] != response.links['next']['url']:
                url = response.links['next']['url']
            elif 'last' in response.links and response.links['current']['url'] != response.links['last']['url']:
                url = response.links['last']['url']
            else:
                done = True

        return submissions


    def teardown(self):
        pass
