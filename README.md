# MLOPS-A2-20I-0873-DS-N

This Airflow DAG automates the process of scraping news articles, performing sentiment analysis, and pushing the data to a Git repository.

## Overview

The DAG consists of three main tasks:
1. **extract_links**: Scrapes news articles, extracts metadata, and performs sentiment analysis.
2. **store_data_in_csv**: Stores the extracted data in a CSV file.
3. **push_to_git**: Pushes the CSV file to a DVC google drive remote and metadata to Git repository.

## Dependencies

- **Airflow**: For workflow automation.
- **Requests and BeautifulSoup**: For web scraping.
- **csv**: For handling CSV files.
- **os**: For interacting with the operating system.
- **subprocess**: For executing shell commands.
- **re**: For text preprocessing.
- **datetime**: For date and time manipulation.

## Steps to Re-Create Results:

- **Airflow Setup**
  ```bash
  airflow db init
  airflow scheduler
  airflow webserver
  ```
- **Paste the main.py file in dags folder of Airflow and Run the DAG with tag a2**
