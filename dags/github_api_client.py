import time
import logging
from typing import Dict, List, Optional, Tuple
import requests
from http import HTTPStatus
from config import config
from utils import retry_on_failure

logger = logging.getLogger(__name__)


class GitHubClient:
    """Client for interacting with GitHub API"""

    def __init__(self):
        self.base_url = config.get_base_url()
        self.headers = config.get_github_headers()
        self.timeout = config.REQUESTS_TIMEOUT

    @retry_on_failure()
    def _make_request(self, url: str, params: Optional[Dict] = None) -> requests.Response:
        """
        Make HTTP GET request with rate limiting handling

        Args:
            url: API endpoint URL
            params: Query parameters

        Returns:
            Response object

        Raises:
            requests.HTTPError: If request fails
        """
        params = params or {}
        response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)

        # Handle rate limiting
        if response.status_code == HTTPStatus.FORBIDDEN and response.headers.get('X-RateLimit-Remaining') == '0':
            reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
            wait_time = max(reset_time - time.time(), 0) + 1
            logger.warning(f'Rate limit exceeded. Waiting {wait_time:.0f}s')
            time.sleep(wait_time)

            response = requests.get(url, params=params, headers=self.headers, timeout=self.timeout)

        response.raise_for_status()
        return response

    def _paginate(self, url_path: str, params=None) -> List[Dict]:
        """
        Fetch all pages of results from API endpoint

        Args:
            url_path: API endpoint path (e.g., '/pulls')
            params: Query parameters

        Returns:
            List of all items from all pages
        """
        if params is None:
            params = {}
        items = []
        page = 1

        while True:
            url = f'{self.base_url}{url_path}'
            params = {
                **params,
                'page': page,
                'per_page': config.PAGINATION_SIZE
            }

            logger.debug(f'Fetching page {page} from {url_path}')
            response = self._make_request(url, params)
            data = response.json()

            if not data:
                break

            items.extend(data)
            logger.info(f'Fetched {len(data)} items from page {page}')

            if len(data) < config.PAGINATION_SIZE:
                break

            page += 1

        logger.info(f'Total items fetched from {url_path}: {len(items)}')
        return items

    def get_pull_requests(self) -> List[Dict]:
        """
        Fetch all pull requests, with state = closed

        Returns:
            List of pull request data
        """
        return self._paginate('/pulls', params={'state': config.CLOSED_STATE})

    def get_reviews(self, pr_number: int) -> List[Dict]:
        """
        Fetch reviews for a pull request

        Args:
            pr_number: Pull request number

        Returns:
            List of review data
        """
        return self._paginate(f'/pulls/{pr_number}/reviews')

    def get_commits(self, pr_number: int) -> List[Dict]:
        """
        Fetch commits for a pull request

        Args:
            pr_number: Pull request number

        Returns:
            List of commit data
        """
        return self._paginate(f'/pulls/{pr_number}/commits')

    def get_commit_status(self, commit_sha: str) -> Tuple[Dict, List[Dict]]:
        """
        Fetch commit status and check runs

        Args:
            commit_sha: Commit SHA hash

        Returns:
            Tuple of (combined_status, check_runs)
        """
        try:
            # Get combined status
            status_url = f'{self.base_url}/commits/{commit_sha}/status'
            status_response = self._make_request(status_url)
            combined_status = status_response.json()

            # Get check runs
            checks_url = f'{self.base_url}/commits/{commit_sha}/check-runs'
            checks_response = self._make_request(checks_url)
            check_runs = checks_response.json().get('check_runs', [])

            return combined_status, check_runs

        except Exception as e:
            logger.warning(f'Failed to fetch status for commit {commit_sha}: {e}')
            return {}, []
