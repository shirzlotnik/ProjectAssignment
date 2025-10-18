import json
import logging
from pathlib import Path
from typing import Any, Dict, List
from jsonschema import validate, ValidationError

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
