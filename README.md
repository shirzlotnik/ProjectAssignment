# GitHub Pull Request Compliance ETL Pipeline

## 🎯 Overview

This pipeline automates the process of extracting pull request data from GitHub, analyzing compliance and calculating summary reports for audit and monitoring purposes.

### 🧱 Design Decision
Started with basic functional programming with python - broke down each part of the assignment into a function.
After i got things to work I started to design the project structure, separate different functions into different files and classes.

The idea was that each class had a specific purpose, one to handle communication with the GitHub API, the other to handle the local storage, and the central one to handle the ETL functions - extract, transform, load.
I wanted functions that are simple but not useless, after a couple of versions, I got this.

The setup for this project is very simple and does not require the other person to know almost anything.


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

3. **Loads** processed data into Parquet files
   - Load to duckdb

## 📦 Prerequisites

- Python 3.8+
- Apache Airflow 2.0+
- GitHub Personal Access Token with repository access

## 🚀 Installation

### 1. Clone the Repository

```bash
git clone https://github.com/shirzlotnik/ProjectAssignment.git
cd ProjectAssignment
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

Edit the variables to your values or set env variables:

```python
    # GitHub Configuration
    GITHUB_TOKEN = os.getenv('GITHUB_TOKEN', '')
    GITHUB_REPO_OWNER = os.getenv('GITHUB_REPO_OWNER', 'Scytale-exercise')
    GITHUB_REPO_NAME = os.getenv('GITHUB_REPO_NAME', 'Scytale_repo')
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
