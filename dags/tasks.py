import logging
from datetime import datetime

import pandas as pd
from models import PullRequest, ComplianceSummary
from utils import write_json, load_schema, validate_json_schema, read_json
from github_api_client import GitHubClient
from config import config

logger = logging.getLogger(__name__)

client = GitHubClient()


def _process_pull_request(pr_data: dict) -> PullRequest:
    """
    Process a single pull request and gather all metadata

    Args:
        pr_data: Raw PR data from GitHub API

    Returns:
        PullRequest object with all metadata
    """
    pr_number = pr_data['number']

    reviews = client.get_reviews(pr_number)
    approved_count = sum(1 for r in reviews if r['state'] == config.APPROVED_STATE)
    reviews_information = [{'state': r['state']} for r in reviews]

    commits = client.get_commits(pr_number)
    # Get last or head commits' SHA
    if commits:
        commit_sha = commits[-1]['sha']
    else:
        commit_sha = pr_data.get('head', {}).get('sha', '')

    combined_status, check_runs = [], []
    if commit_sha:
        combined_status, check_runs = client.get_commit_status(commit_sha)
    check_runs_data = [{
        'name': cr['name'],
        'conclusion': cr['conclusion'],
        'completed_at': cr['completed_at']
    } for cr in check_runs]

    return PullRequest(
        number=pr_number,
        title=pr_data['title'],
        user_login=pr_data['user']['login'],
        user_id=pr_data['user']['id'],
        repository=config.GITHUB_REPO_NAME,
        created_at=pr_data['created_at'],
        merged_at=pr_data['merged_at'],
        closed_at=pr_data['closed_at'],
        base_branch=pr_data['base']['ref'],
        head_branch=pr_data['head']['ref'],
        reviews=reviews_information,
        approved_reviews=approved_count,
        check_runs=check_runs_data
    )


def extract(**kwargs):
    """
    Extract all pull request data

    Returns:
        List of pull request dictionaries
    """
    run_id = kwargs['dag_run'].run_id

    pull_requests = client.get_pull_requests()
    logger.info(f'Found {len(pull_requests)} closed pull requests')

    raw_path = config.OUTPUT_DIR / f'raw_pull_requests{run_id}.json'
    write_json(raw_path, pull_requests)

    processed_prs = [_process_pull_request(pr).to_dict() for pr in pull_requests]

    processed_prs_path = config.OUTPUT_DIR / f'processed_pull_requests_{run_id}.json'
    write_json(processed_prs_path, processed_prs)

    kwargs['ti'].xcom_push(key='processed_prs_path', value=str(processed_prs_path))


def _calculate_summary(df: pd.DataFrame) -> ComplianceSummary:
    """
    Calculate compliance summary statistics

    Args:
        df: DataFrame with compliance data

    Returns:
        ComplianceSummary object
    """
    total_prs = len(df)
    compliant_prs = int(df['is_compliant'].sum())
    compliance_rate = compliant_prs / total_prs if total_prs > 0 else 0.0

    summary = ComplianceSummary(
        total_prs=total_prs,
        compliant_prs=compliant_prs,
        compliance_rate=compliance_rate
    )

    logger.info('Compliance Summary:')
    logger.info(summary.to_dict())

    return summary


def transform(**kwargs):
    """
    Transform PR data and calculate compliance

    Returns:
        DataFrame of transformed data
    """
    run_id = kwargs['dag_run'].run_id
    processed_prs_path = kwargs['ti'].xcom_pull(key='processed_prs_path', task_ids='extract')
    data = read_json(processed_prs_path)
    schema = load_schema(config.SCHEMA_PATH)

    is_valid = validate_json_schema(data, schema)
    if not is_valid:
        raise ValueError('Data does not match expected schema')

    df = pd.DataFrame(data)
    if df.empty:
        logger.warning('No data to transform')
        return df

    # Apply logic for validation and complaince
    df['status_checks_passed'] = (df['check_runs'].apply(
        lambda check_runs: all(c['conclusion'] == config.REQUIRED_CHECK_CONCLUSION
                               for c in check_runs)))
    columns_rename = {'number': 'pr_number', 'title': 'pr_title',
                      'approved_reviews': 'code_review_passed',
                      'author': 'user_login'}
    # For the required output
    df.rename(columns=columns_rename, inplace=True)
    df['is_compliant'] = df['code_review_passed'] & df['status_checks_passed']

    summary_path = config.OUTPUT_DIR / f'summary_{run_id}.json'
    write_json(summary_path, _calculate_summary(df).to_dict())

    return df


def load(**kwargs):
    """
    Load the transformed data from DataFrame to parquet file
    """
    data = kwargs['ti'].xcom_pull(key='return_value', task_ids='transform')
    logger.info('Extracted the data from XCom')

    df = pd.DataFrame(data)
    now = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
    parquet_path = config.OUTPUT_DIR / f'pull_requests_{now}.parquet'

    df.to_parquet(parquet_path, index=False)
    logging.info(f'Saved Parquet to {parquet_path}')
