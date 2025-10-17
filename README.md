# GitHub Pull Request Compliance ETL Pipeline

A robust ETL (Extract, Transform, Load) pipeline built with Apache Airflow to monitor and analyze pull request compliance across GitHub repositories.

## 🎯 Overview

This pipeline automates the process of extracting pull request data from GitHub, analyzing compliance with code review and testing standards, and generating reports for audit and monitoring purposes.

### What It Does

1. **Extracts** pull request data from GitHub repositories including:
   - Pull request metadata
   - Code reviews and approvals
   - Commit history
   - Check runs

2. **Transforms** the data to calculate:
   - Compliance status per pull request
   - Approval rates
   - Violations by repository

3. **Loads** processed data into Parquet files for:
   - Long-term storage
   - Compliance audits

## 📦 Prerequisites

- Python 3.8+
- Apache Airflow 2.0+
- GitHub Personal Access Token with repository access

### Required Python Packages

```
apache-airflow>=2.0.0
requests>=2.28.0
pandas>=1.5.0
jsonschema>=4.17.0
pyarrow>=10.0.0
```

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/shirzlotnik/ProjectAssignment.git
cd github-pr-compliance-etl
```

### 2. Open with PyCharm


### 3. Install Dependencies

```bash
pip install -r requirements.txt
```


## ⚙️ Configuration

### 1. GitHub Token Setup

Create a GitHub Personal Access Token:

1. Go to GitHub Settings → Developer settings → Personal access tokens
2. Click "Generate new token"
3. Select scopes: `repo` (Full control of private repositories)
4. Copy the generated token

### 2. Update Configuration

Edit the variables to your values:

```python
    REPO_OWNER: str = 'your-org'        # Your GitHub organization
    REPO_NAME: str = 'your-repo'        # Your repository name
    GITHUB_TOKEN: str = 'your-token'  # Optional second repo
```

## 📖 Usage

### Start Airflow

```bash
docker-compose up
```
username & password are `airflow`

### Access Web UI

1. Open browser to `http://localhost:8080`
2. Login with your admin credentials
3. Find the DAG: `compliance_etl`
4. Toggle the DAG to "On"

### Manual Trigger

```bash
# Via Web UI
Click the "Play" button on the DAG
```

### View Results

```bash
# Check output files
ls dags/output/

```
