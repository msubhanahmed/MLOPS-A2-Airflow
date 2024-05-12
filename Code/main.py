from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import requests
from bs4 import BeautifulSoup
import csv
import os 
import subprocess
import re
from datetime import datetime, timedelta


default_args = {'owner': 'airflow', 'depends_on_past': False, 'email_on_failure': False, 'email_on_retry': False, 'retries': 1, }
dag = DAG( 'MLOPS-A2', default_args=default_args, description='An Airflow DAG to Scrap News Articles and Push to Git', schedule_interval='@daily', start_date=days_ago(1), tags=['a2'])


def push_to_github():
    print("Directory", os.getcwd()) ## Make Sure to Start from MLOPS/A2

    command = 'cd .. && cd Data && dvc add data.csv && dvc push && git add . && git commit -m "Automated Push" && git push'
    try:
        result = subprocess.check_output(command, shell=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")





def loadandTransform():
    def sentiment_analysis(text):
        try:
            API_URL = "https://api-inference.huggingface.co/models/mrm8488/distilroberta-finetuned-financial-news-sentiment-analysis"
            headers = {"Authorization": "Bearer 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"}
            def query(payload):
                response = requests.post(API_URL, headers=headers, json=payload)
                return response.json()
                
            output = query({"inputs": text})
            print("Sentiment", output[0][0]['label'])
            return output[0][0]['label']
        except Exception as E:
            print(E)
            return "** None **"    

    def preprocess_text(text):
        if text is None:
                return None
        clean_text = re.sub('<.*?>', ' ', text)
        clean_text = re.sub(r'[^ -~]', ' ', clean_text)
        clean_text = clean_text.lower()
        clean_text = re.sub(r'\s+', ' ', clean_text)
        return clean_text.strip()
    
    sources = ['https://www.dawn.com/latest-news','https://www.bbc.com/news' ]
    data = []
    for source in sources:
        response = requests.get(source)
        if response.status_code == 200:
            soup        = BeautifulSoup(response.text, 'html.parser')
            articles    = soup.find_all('article')

            for idx, article in enumerate(articles):
                title       = preprocess_text(article.find('h2').text.strip() if article.find('h2') else None)
                description = preprocess_text(article.text.strip() if len(article.text.strip()) else None)

                ## Preprocessing Check to filter out the correct data
                if not title or not description:
                    continue
                if len(title) < 10 or len(description) < 10:
                    continue

                ## Save the valid data
                data.append({'id': idx+1, 'sentiment':sentiment_analysis(description), 'title': title, 'description': description, 'source': source})

    return data

def savetoCSV(**kwargs):
    directory = "/home/msa/Desktop/MLOPS/Data"
    if not os.path.exists(directory):
        os.makedirs(directory)

    data = kwargs['ti'].xcom_pull(task_ids='extract_links')
    if not data:
        raise ValueError("No Data Provided")
    
    csv_file = os.path.join(directory,'data.csv')
    try:
        with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['id', 'sentiment', 'title', 'description', 'source']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for item in data:
                writer.writerow(item)
                print("*"*10, item,"*"*10)
        print("Data successfully stored")
    except Exception as e:
        print(f"Error occurred while saving data: {e}")
        raise e

t1      = PythonOperator( task_id='extract_links',      python_callable=loadandTransform,   execution_timeout=timedelta(seconds=180) ,    dag=dag )
t2      = PythonOperator( task_id='store_data_in_csv',  python_callable=savetoCSV,          provide_context=True, dag=dag,)
t3      = PythonOperator( task_id='push_to_git',        python_callable=push_to_github,                dag=dag )

t1 >> t2 >> t3
