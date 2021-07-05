from __future__ import absolute_import

import argparse
import logging

import apache_beam as beam
#from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from endpoints.assignments import Assignments, AssignmentsTransform
from endpoints.courses import Courses, CoursesTransform
from endpoints.enrollments import Enrollments, EnrollmentsTransform
from endpoints.sections import Sections, SectionsTransform
from endpoints.submissions import Submissions, SubmissionsTransform
from endpoints.terms import Terms, TermsTransform
from endpoints.users import Users, UsersTransform


def run(pipeline_args, base_url, endpoint, start_date, token):

    """Main entry point; defines and runs the pipeline."""
    
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    project_id = pipeline_options.get_all_options()['project']

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as pipeline:

        if endpoint == 'assignments':
            endpoint = Assignments
            transform = AssignmentsTransform
            table_id = f'Assignments'
            query = f'SELECT DISTINCT id FROM `{project_id}.Canvas.Courses`'
        elif endpoint == 'courses':
            endpoint = Courses
            transform = CoursesTransform
            table_id = f'Courses'
            query = f'SELECT id FROM `{project_id}.Canvas.Terms` WHERE DATE(start_at) >= DATE "{start_date}"'
        elif endpoint == 'enrollments':
            endpoint = Enrollments
            transform = EnrollmentsTransform
            table_id = f'Enrollments'
            query = f'SELECT DISTINCT id FROM `{project_id}.Canvas.Courses`'
        elif endpoint == 'sections':
            endpoint = Sections
            transform = SectionsTransform
            table_id = f'Sections'
            query = f'SELECT DISTINCT id FROM `{project_id}.Canvas.Courses`'
        elif endpoint == 'submissions':
            endpoint = Submissions
            transform = SubmissionsTransform
            table_id = f'Submissions'
            query = f'SELECT DISTINCT id, course_id FROM `{project_id}.Canvas.Assignments`'
        elif endpoint == 'terms':
            endpoint = Terms
            transform = TermsTransform
            table_id = f'Terms'
            # query result not used
            query = f'SELECT GENERATE_ARRAY(1, 1)'
        elif endpoint == 'users':
            endpoint = Users
            transform = UsersTransform
            table_id = f'Users'
            query = f'SELECT DISTINCT user_id FROM `{project_id}.Canvas.Enrollments`'
        else:
            raise
        

        canvas = (
            pipeline 
            | 'Read Course IDs from bigquery' >> beam.io.Read(
                beam.io.BigQuerySource(
                    query=query,
                    validate=True,
                    use_standard_sql=True
                )
            )
            | 'Get data from Canvas API' >> beam.ParDo(endpoint(base_url, endpoint, token))
            | 'Parse JSON into pcollection' >> beam.ParDo(transform(known_args))
            | 'Load into BigQuery' >> beam.io.WriteToBigQuery(
                bigquery.TableReference(
                    projectId=project_id,
                    datasetId='Canvas',
                    tableId=table_id
                ),
                schema=endpoint().table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )
    
        

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--base_url',
        dest='base_url',
        required=True,
        help='Canvas base url')
    parser.add_argument(
        '--endpoint',
        dest='endpoint',
        required=True,
        help='Canvas API endpoint')
    parser.add_argument(
        '--start_date',
        dest='start_date',
        required=False,
        help='School year start date')
    parser.add_argument(
        '--token',
        dest='token',
        required=True,
        help='Canvas API token')
    known_args, pipeline_args = parser.parse_known_args()
    run(pipeline_args, known_args.base_url, known_args.endpoint,
        known_args.start_date, known_args.token)
