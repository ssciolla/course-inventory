# standard libraries
import json, logging, os
from datetime import datetime
from json.decoder import JSONDecodeError
from typing import Dict, Optional, Sequence, Union

# third-party libraries
import pandas as pd
import psycopg2
import requests
from requests import Response
from umich_api.api_utils import ApiUtil

# local libraries
from gql_queries import queries as QUERIES

# Initialize settings and globals

logger = logging.getLogger(__name__)

try:
    with open(os.path.join('config', 'env.json')) as env_file:
        ENV = json.loads(env_file.read())
except FileNotFoundError:
    logger.error('Configuration file could not be found; please add env.json to the config directory.')

logging.basicConfig(level=ENV.get('LOG_LEVEL', 'DEBUG'))

API_UTIL = ApiUtil(ENV['API_BASE_URL'], ENV['API_CLIENT_ID'], ENV['API_CLIENT_SECRET'])
SUBSCRIPTION_NAME = ENV['API_SUBSCRIPTION_NAME']
API_SCOPE_PREFIX = ENV['API_SCOPE_PREFIX']
MAX_REQ_ATTEMPTS = ENV['MAX_REQ_ATTEMPTS']
ACCESS_TOKEN = ENV['ACCESS_TOKEN']

UDW_CONN = psycopg2.connect(**ENV['UDW'])
WAREHOUSE_INCREMENT = ENV['WAREHOUSE_INCREMENT']


# Function(s)

def make_request_using_api_utils(
    url: str,
    params: Dict[str, Union[str, int]] = {}, 
    method: Optional[str] = None
) -> Response:

    logger.debug('Making a request for data...')

    for i in range(1, MAX_REQ_ATTEMPTS + 1):
        logger.debug(f'Attempt #{i}')
        if method:
            response = requests.request(method=method, url=url, json=params)
        else:
            response = requests.get(url=url, params=params)
        # if method:
        #     response = API_UTIL.api_call(url, SUBSCRIPTION_NAME, payload=params, method=method)
        # else:
        #     response = API_UTIL.api_call(url, SUBSCRIPTION_NAME, payload=params)
        logger.info('Received response with the following URL: ' + response.url)
        status_code = response.status_code

        if status_code != 200:
            logger.warning(f'Received irregular status code: {status_code}')
            logger.info('Beginning next_attempt')
        else:
            try:
                response_data = json.loads(response.text)
                return response
            except JSONDecodeError:
                logger.warning('JSONDecodeError encountered')
                logger.info('Beginning next attempt')

    logger.error('The maximum number of request attempts was reached')
    return response


def slim_down_course_data(course_data: Sequence[Dict]) -> Sequence[Dict]:
    course_dicts = course_data['data']['term']['coursesConnection']['nodes']

    slim_course_dicts = []
    for course_dict in course_dicts:
        slim_course_dict = {
            'course_id': int(course_dict['_id']),
            'course_name': course_dict['name'],
            'course_account_id': course_dict['account']['_id'],
            'course_created_at': course_dict['createdAt'],
            'course_workflow_state': course_dict['state']
        }
        slim_course_dicts.append(slim_course_dict)
    return slim_course_dicts


def gather_course_info_for_account(account_id: int, term_id: int) -> Sequence[int]:
    start = datetime.now()
    url_ending_with_scope = f'{API_SCOPE_PREFIX}/api/graphql'
    url_ending = 'https://umich.instructure.com/api/graphql'
    params = {
        'access_token': ACCESS_TOKEN,
        'query': QUERIES['coursesQuery'],
        'variables': {
            'termID': ENV['TERM_ID'],
            'pageSize': 100,
            'pageCursor': ''
        }
    }

    more_pages = True
    page_num = 1
    slim_course_dicts = []

    while more_pages:
        logger.info(f'Course Page Number: {page_num}')
        response = make_request_using_api_utils(url_ending, params, method='POST')
        all_course_data = json.loads(response.text)
        slim_course_dicts += slim_down_course_data(all_course_data)
        page_info_dict = all_course_data['data']['term']['coursesConnection']['pageInfo']

        if page_info_dict['hasNextPage']:
            page_num += 1
            page_cursor = page_info_dict['endCursor']
            params['variables']['pageCursor'] = page_cursor
        else:
            logger.info('No more pages!')
            more_pages = False

    course_df = pd.DataFrame(slim_course_dicts)
    course_df['course_warehouse_id'] = course_df['course_id'].map(lambda x: x + WAREHOUSE_INCREMENT)
    logger.debug(course_df.head())
    course_df.to_csv(os.path.join('data', 'course.csv'), index=False)
    logger.info('Course data was written to data/course.csv')
    course_ids = course_df['course_warehouse_id'].to_list()

    delta = datetime.now() - start
    logger.info(delta.total_seconds())
    return course_ids


def pull_enrollment_and_user_data(udw_course_ids) -> None:
    udw_courses_string = ','.join([str(udw_course_id) for udw_course_id in udw_course_ids])
    enrollment_query = f'''
        SELECT e.id AS enrollment_id,
               e.canvas_id AS enrollment_canvas_id,
               e.user_id AS enrollment_user_id,
               e.course_section_id AS enrollment_course_section_id,
               e.course_id AS enrollment_course_id,
               e.workflow_state AS enrollment_workflow_state,
               r.base_role_type AS role_type
        FROM enrollment_dim e
        JOIN role_dim r
            ON e.role_id=r.id
        WHERE e.course_id IN ({udw_courses_string});
    '''

    logger.info('Making enrollment_dim query')
    enrollment_df = pd.read_sql(enrollment_query, UDW_CONN)

    enrollment_df.to_csv(os.path.join('data', 'enrollment.csv'), index=False)
    logger.info('Enrollment data was written to data/enrollment.csv')

    user_ids = enrollment_df['enrollment_user_id'].drop_duplicates().to_list()
    users_string = ','.join([str(user_id) for user_id in user_ids])

    user_query = f'''
        SELECT u.id AS user_id,
               u.canvas_id AS user_canvas_id,
               u.name AS user_name,
               u.workflow_state AS user_workflow_state,
               p.unique_name AS pseudonym_uniqname
        FROM user_dim u
        JOIN pseudonym_dim p
            ON u.id=p.user_id
        WHERE u.id in ({users_string});
    '''

    logger.info('Making user_dim query')
    user_df = pd.read_sql(user_query, UDW_CONN)
    user_df.to_csv(os.path.join('data', 'user.csv'), index=False)
    logger.info('User data was written to data/user.csv')


if __name__ == "__main__":
    current_udw_course_ids = gather_course_info_for_account(1, ENV['TERM_ID'])
    pull_enrollment_and_user_data(current_udw_course_ids)
