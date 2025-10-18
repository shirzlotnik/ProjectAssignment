import json
import logging
import time
import pandas as pd

from pathlib import Path
from typing import Any, Dict, List
from jsonschema import validate, ValidationError
from config import config
from functools import wraps
from models import ComplianceSummary

logger = logging.getLogger(__name__)


def read_json(file_path: Path) -> Any:
    """
    Read JSON file with error handling

    Args:
        file_path: Path to JSON file

    Returns:
        Parsed JSON data

    Raises:
        FileNotFoundError: If file doesn't exist
        json.JSONDecodeError: If file contains invalid JSON
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        logger.info(f'Successfully read JSON from {file_path}')
        return data
    except FileNotFoundError:
        logger.error(f'File not found: {file_path}')
        raise
    except json.JSONDecodeError as e:
        logger.error(f'Invalid JSON in {file_path}: {e}')
        raise


def write_json(file_path: Path, data: Any) -> None:
    """
    Write data to JSON file with error handling

    Args:
        file_path: Path to output file
        data: Data to write

    Raises:
        IOError: If write fails
    """
    try:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        logger.info(f'Successfully wrote JSON to {file_path}')
    except Exception as e:
        logger.error(f'Failed to write JSON to {file_path}: {e}')
        raise


def validate_json_schema(data: List[Dict], schema: Dict) -> bool:
    """
    Validate JSON data against schema

    Args:
        data: Data to validate
        schema: JSON schema

    Returns:
        True if valid, False otherwise
    """
    try:
        validate(instance=data, schema=schema)
        logger.info('JSON schema validation passed')
        return True
    except ValidationError as e:
        logger.error(f'Schema validation failed: {e.message}')
        return False
    except Exception as e:
        logger.error(f'Unexpected validation error: {e}')
        return False


def load_schema(schema_path: Path) -> Dict:
    """
    Load JSON schema with fallback

    Args:
        schema_path: Path to schema file

    Returns:
        JSON schema dictionary
    """
    try:
        return read_json(schema_path)
    except Exception as e:
        logger.warning(f'Could not load schema from {schema_path}: {e}. Using basic schema.')
        raise


def retry_on_failure(max_attempts: int = config.MAX_RETRIES, delay: int = config.RETRY_DELAY):
    """
    Decorator to retry function on failure with exponential backoff

    Args:
        max_attempts: Maximum number of retry attempts
        delay: Base delay in seconds between retries
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        logger.error(f'{func.__name__} failed after {max_attempts} attempts')
                        raise

                    wait_time = delay * (2 ** attempt)
                    logger.warning(
                        f'{func.__name__} attempt {attempt + 1}/{max_attempts} failed: {e}. '
                        f'Retrying in {wait_time}s...'
                    )
                    time.sleep(wait_time)

        return wrapper

    return decorator


def calculate_summary(df: pd.DataFrame) -> ComplianceSummary:
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
