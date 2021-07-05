import logging
import requests

import apache_beam as beam


class AssignmentsTransform(beam.DoFn):
    def __init__(self, known_args):
        pass

    # runs for each record in pcollection
    def process(self, text):
        output = dict()

        # iterate through BigQuery columns
        for key in [record['name'] for record in Assignments.table_schema['fields']]:   

            output[key] = text.get(key)

        yield output


class Assignments(beam.DoFn):

    table_schema = {
        'fields': [
            {'name': 'id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
            {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'due_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'unlock_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'lock_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'points_possible', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'grading_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'assignment_group_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'grading_standard_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'created_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'updated_at', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'peer_reviews', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'automatic_peer_reviews', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'position', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'grade_group_students_individually', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'anonymous_peer_reviews', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'group_category_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'post_to_sis', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'moderated_grading', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'omit_from_final_grade', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'intra_group_peer_reviews', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'anonymous_instructor_annotations', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'anonymous_grading', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'graders_anonymous_to_graders', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'grader_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'grader_comments_visible_to_graders', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'final_grader_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'grader_names_visible_to_final_grader', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'allowed_attempts', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'secure_params', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'course_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'submission_types', 'type': 'STRING', 'mode': 'REPEATED'},
            {'name': 'has_submitted_submissions', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'due_date_required', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'max_name_length', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'in_closed_grading_period', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'is_quiz_assignment', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'can_duplicate', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'original_course_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'original_assignment_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'original_assignment_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'original_quiz_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'workflow_state', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'muted', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'html_url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'has_overrides', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'needs_grading_count', 'type': 'INTEGER', 'mode': 'NULLABLE'},
            {'name': 'published', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'unpublishable', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'only_visible_to_overrides', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'locked_for_user', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'submissions_download_url', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'post_manually', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'anonymize_students', 'type': 'BOOLEAN', 'mode': 'NULLABLE'},
            {'name': 'require_lockdown_browser', 'type': 'BOOLEAN', 'mode': 'NULLABLE'}
        ]
    }

    def __init__(self, base_url=None, endpoint=None, token=None):
        self.base_url = base_url
        self.endpoint = endpoint
        self.token = token

    def setup(self):
        pass

    def process(self, element):

        logging.info(f"Fetching assignments for course id: {element['id']}")

        course_id = element['id']
        assignments = list()

        done = False
        per_page = 100
        url = f'{self.base_url}/api/v1/courses/{course_id}/assignments?page=1&per_page={per_page}'

        while not done:

            response = requests.get(
                url,
                headers={'Authorization' : f'Bearer {self.token}'})
            
            if isinstance(response.json(), list):
                logging.info(f'Retrieved  {len(response.json())} assignments')
                assignments = assignments + response.json()
            else:
                logging.warn(f'Course id {course_id} did not return an assignments list')

            if 'next' in response.links and response.links['current']['url'] != response.links['next']['url']:
                url = response.links['next']['url']
            elif 'last' in response.links and response.links['current']['url'] != response.links['last']['url']:
                url = response.links['last']['url']
            else:
                done = True

        return assignments


    def teardown(self):
        pass
