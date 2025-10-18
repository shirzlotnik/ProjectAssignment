import json
import requests
import logging
import pandas as pd
from jsonschema import validate, ValidationError
import time
from datetime import datetime
import os
import uuid

REPO_OWNER = 'Scytale-exercise'
REPO_NAME = 'Scytale_repo'
REPO_NAME2 = 'scytale-repo2'
APPROVED = 'APPROVED'
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
# add your token
GITHUB_TOKEN = None
GITHUB_AUTH_HEADER = {
    'authorization': 'token {0}'.format(GITHUB_TOKEN)
}
base_url = f'https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}'


def json_schema_validation(json_data: dict, schema: dict) -> bool:
    try:
        validate(instance=json_data, schema=schema)
        logging.info('JSON is valid')
        return True
    except ValidationError as e:
        logging.error(f'JSON not valid, {e}')
        return False
    except Exception as e:
        logging.error(f'Error while validating, {e}')
        return False


def handle_json_write(path, data):
    with open(path, 'w') as json_file:
        json.dump(data, json_file, indent=2)


def handle_json_read(path):
    with open(path, 'r') as file:
        data = json.load(file)
        return data


pull_requests_schema = handle_json_read(os.path.join('dags', 'schema.json'))


def handle_get_request(url: str, params=None, headers=None):
    if params is None:
        params = {}
    if headers is None:
        headers = GITHUB_AUTH_HEADER
    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 403 and response.headers.get('X-RateLimit-Remaining') == '0':
        reset_ts = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
        wait = max(reset_ts - time.time(), 0) + 1
        logging.warning(f'Rate limit exceeded, sleeping {wait:.0f}s')
        time.sleep(wait)
        response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response


def handle_pagination(url_extension):
    try:
        elements = []
        page = 1
        per_page = 30
        while True:
            url = f'{base_url}{url_extension}'
            params = {'state': 'closed', 'page': page, 'per_page': per_page}
            response = handle_get_request(url, params=params)
            data = response.json()
            if not data:
                break
            elements.extend(data)
            page += 1
        return elements
    except Exception as e:
        logging.error(f'Error paginating, {e}')


def fetch_status_and_check_runs(commit_sha: str):
    url = f'{base_url}/commits/{commit_sha}/status'
    response_status = handle_get_request(url)
    combined_status = response_status.json()

    runs_url = f'{base_url}/commits/{commit_sha}/check-runs'
    response_runs = handle_get_request(runs_url, headers={**GITHUB_AUTH_HEADER,
                                                          'Accept': 'application/vnd.github.v3+json'})
    check_runs = response_runs.json().get('check_runs', [])
    return combined_status, check_runs


def extract(**kwargs):
    try:
        raw_pull_requests_path = os.path.join('dags', 'output', f'pull_requests_raw_{uuid.uuid4()}.json')

        logging.info('Getting all the pull requests from the repo')
        pull_requests = handle_pagination('/pulls')
        logging.debug('Writing raw data to file')
        handle_json_write(raw_pull_requests_path, pull_requests)
        pull_requests_metadata = []

        logging.info('Iterating the pull requests')
        for pr_metadata in pull_requests:
            pr_number = pr_metadata['number']

            logging.info(f'Getting pr_number={pr_number} reviews')
            reviews = handle_pagination(f'/pulls/{pr_number}/reviews')
            approved = sum(1 for r in reviews if r['state'] == 'APPROVED')

            logging.info(f'Getting pr_number={pr_number} commits')
            commits = handle_pagination(f'/pulls/{pr_number}/commits')

            if len(commits):
                last_commit = commits[-1]
                commit_sha = last_commit['sha']
            else:
                commit_sha = pr_metadata.get('head', {})['sha']

            combined_status, check_runs = {}, []
            if commit_sha:
                logging.info(f'Getting pr_number={pr_number} combined status and check runs')
                combined_status, check_runs = fetch_status_and_check_runs(commit_sha)
            reviews_info = list(map(lambda x: {'state': x['state']}, reviews))

            check_runs_list = list(map(lambda x: {
                'name': x['name'], 'conclusion': x['conclusion'],
                'completed_at': x['completed_at']}, check_runs))

            pull_requests_metadata.append({
                'number': pr_number,
                'title': pr_metadata['title'],
                'user_login': pr_metadata['user']['login'],
                'user_id': pr_metadata['user']['id'],
                'repository': REPO_NAME,
                'merged_at': pr_metadata['merged_at'],
                'base_branch': pr_metadata['base']['ref'],
                'head_branch': pr_metadata['head']['ref'],
                'reviews': reviews_info,
                'approved_reviews': approved,
                'check_runs': check_runs_list,
            })

        pull_requests_path = os.path.join('dags', 'output', f'pull_requests_{uuid.uuid4()}.json')
        handle_json_write(pull_requests_path, pull_requests_metadata)

        kwargs['ti'].xcom_push(key='raw_prs_json', value=pull_requests)
        kwargs['ti'].xcom_push(key='prs_path', value=pull_requests_path)

    except Exception as e:
        logging.error(e)
        print(f'An error occurred: {e}')


def transform(**kwargs):
    try:
        pull_requests_path = kwargs['ti'].xcom_pull(key='prs_path', task_ids='extract')
        data = handle_json_read(pull_requests_path)
        valid = json_schema_validation(data, pull_requests_schema)
        if valid:
            logging.info('Creating DataFrame from JSON')
            df = pd.DataFrame(data)
            df['status_checks_passed'] = df['check_runs'].apply(
                lambda checks: all(c['conclusion'] == 'success' for c in checks))
            df.rename(columns={'number': 'pr_number', 'title': 'pr_title',
                               'approved_reviews': 'code_review_passed',
                               'author': 'user_login'}, inplace=True)
            df['is_compliant'] = df['code_review_passed'] & df['status_checks_passed']

            total_prs = len(df)
            compliant = df['is_compliant'].sum()
            compliance_rate = compliant / total_prs if total_prs > 0 else 0.0
            violations_by_repo = df.groupby('repository')['is_compliant'].size()

            logging.info('Calculating summary for complaince')
            summary = {
                'total_prs': total_prs,
                'compliance_rate': float(compliance_rate),
                'violations_by_repo': violations_by_repo
            }

            return df

        else:
            logging.error(f'JSON not in schema')

    except Exception as e:
        logging.error(f'Error while transforming, {e}')


def load(**kwargs):
    records = kwargs['ti'].xcom_pull(key='return_value', task_ids='transform')
    df = pd.DataFrame(records)
    df['timestamp'] = datetime.now()
    now = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    file_name = f'pr_compliance_{now}.parquet'
    full_path = os.path.join('dags', 'output', file_name)

    df.to_parquet(full_path, index=False)
    logging.info(f'Saved Parquet to {full_path}')
    kwargs['ti'].xcom_push(key='result_path', value=full_path)
