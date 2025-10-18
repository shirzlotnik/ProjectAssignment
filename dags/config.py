import os
from pathlib import Path
from typing import Dict


class Config:
    """Central configuration for the ETL pipeline"""

    # GitHub Configuration
    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN', '')
    GITHUB_REPO_OWNER = os.getenv('GITHUB_REPO_OWNER', 'Scytale-exercise')
    GITHUB_REPO_NAME = os.getenv('GITHUB_REPO_NAME', 'Scytale_repo')

    # API Configuration
    REQUESTS_TIMEOUT = 30
    MAX_RETRIES = 3
    RETRY_DELAY = 5
    PAGINATION_SIZE = 100
    CLOSED_STATE = 'closed'

    # File Paths
    BASE_DIR = Path(__file__).parent.parent
    DAGS_DIR = BASE_DIR / 'dags'
    OUTPUT_DIR = DAGS_DIR / 'output'
    SCHEMA_PATH = DAGS_DIR / 'schema.json'

    # Compliance Rules
    MIN_APPROVALS_REQUIRED = 1
    REQUIRED_CHECK_CONCLUSION = 'success'
    APPROVED_STATE = 'APPROVED'

    # Logging
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    @classmethod
    def get_github_headers(cls) -> Dict[str, str]:
        if not cls.GITHUB_TOKEN:
            raise ValueError("GITHUB_TOKEN not configured")

        return {
            'Authorization': f'Bearer {cls.GITHUB_TOKEN}',
            'Accept': 'application/vnd.github.v3+json'
        }

    @classmethod
    def get_base_url(cls) -> str:
        return f'https://api.github.com/repos/{cls.GITHUB_REPO_OWNER}/{cls.GITHUB_REPO_NAME}'


config = Config()
