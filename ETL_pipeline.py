#!/usr/bin/python3


#import necessary libraries
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

#ETL scheduling with airflow DAG
default_args = {
    'owner': 'Miiri',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 2, 22, 52),
    'email': ['lillian.wanjiru@student.moringaschool.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'customers_purchases',
    default_args=default_args,
    description='ETL pipeline for customer purchases',
    schedule_interval=timedelta(seconds=10),
)

#path to CSV files
purchases = '/home/vagrant/Billing_Data_Pipeline/dataset1.csv'
payments = '/home/vagrant/Billing_Data_Pipeline/dataset2.csv'
returns = '/home/vagrant/Billing_Data_Pipeline/dataset3.csv'

def extract_data(path_to_csv):
    df = pd.read_csv(path_to_csv)
    return(df)

def clean_purchase_data(df):
    """ cleans data in the purchase dataset"""
    #standardize column names
    df.columns = df.columns.str.replace(' ', '_').str.lower() 
    #Replace columns with missing promo data
    df['promo_code'] =df['promo_code'].fillna('NONE')
    df.drop_duplicates(subset=['customer_id', 'date_of_purchase','country_of_purchase'])
    return(df)

def merge_data(purchase_data,payment_data, refund_data):
    """ merges the 3 dataframes into one using the customer id field"""    
    #merge the data on customer_id columns
    merged_data = pd.merge(purchase_data, pd.merge(payment_data, refund_data, on='customer_id'), on='customer_id')
    # drop columns that contain same information in the merged data frame
    merged_data.drop(['payment_method_y', 'payment_status_y'], axis=1, inplace=True)
    #rename columns
    merged_data = merged_data.rename(columns={'payment_status_x':'payment_status','payment_method_x':'payment_method'})
    
    return(merged_data)

def transform_data(df):
    """ transforms merged dataframe by removing outliers and converting data types"""
    #standardize column names
    df.columns = df.columns.str.replace(' ', '_').str.lower()
    #convert date columns to datetype
    data['date_of_purchase'] = pd.to_datetime(data['date_of_purchase'], format='%m/%d/%Y')
    data['date_of_refund'] = pd.to_datetime(data['date_of_refund'], format='%m/%d/%Y')
    #sort values by customer ID and date of purchase
    data.sort_values(by=['customer_id', 'date_of_purchase'])  

    #check for outliers
    #get the lower and upper bound
    q1 = np.percentile(data['amount_paid'], 5)
    q3 = np.percentile(data['amount_paid'],95)
    iqr = q3-q1
    lower_bound = iqr - 1.5*iqr
    upper_bound = iqr + 1.5*iqr
    #filter out the outliers if any
    df = df.loc[(df['amount_paid']>lower_bound)&(data['amount_paid']<upper_bound),]

    return df

def load_data(df):
    """ loads data to posgresql database"""
    conn = psycopg2.connect(host='localhost', port=5432, database = 'customers', user='postgres', password='Admin')
    cur = conn.cursor()
    #create table
    cur.execute('''create table if not exists customers_purchases 
                (customer_id integer, date_of_purchase date, country_of_purchase text, 
                amount_paid integer, payment_status text, payment_method text, promo_code text, 
                late_payment_fee integer, date_of_refund date, refund_amount integer, reason_for_refund text)''')
    #load data to sql
    engine = create_engine('postgresql+psycopg2://postgres:admin@localhost:5432/customers')
    data.to_sql(name = 'customers_purchases', con= engine, index=False, if_exists='append')
    conn.commit()
    #close cursor and connection
    cur.close()
    conn.close()

#validate the data
def validate_loaded_data():
    #connect to database
    conn = psycopg2.connect(host='localhost', port=5432, database = 'customers', user='postgres', password='Admin')
    cur = conn.cursor()
    #fetch records where amount paid >75
    cur.execute('select customer_id, country_of_purchase, amount_paid from customers_purchases where amount_paid >75')
    results = cur.fetchall()
    #close cursor and connection
    cur.close()
    conn.close()
    
    data = pd.DataFrame(results, columns=['customer_id', 'country_of_purchase', 'amount_paid'])
    return data


if __name__ == '__main__':
    #extract data from csv to dataframe
    purchases_df = extract_data(purchases)
    payment_df = extract_data(payments)
    refund_df = extract_data(returns)

    #clean purchases data
    purchases_df = clean_purchase_data(purchases_df)

    #merge the dataframes
    merged_df = merge_data(purchases_df, payment_df, refund_df)

    #transform merged dataframe
    final_df = transform_data(merged_df)

    #load data into postgres database
    load_data(final_df)
    #validate data
    test_data = validate_loaded_data()
    print(test_data)

    #Schedule ETL with Airflow
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        dag=dag,
    )

    clean = PythonOperator(
        task_id='clean_purchase_data',
        python_callable=clean_purchase_data,
        dag=dag,
    )

    merge = PythonOperator(
        task_id='merge_data',
        python_callable=merge_data,
        dag=dag,
    )
    transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        dag=dag,
    )
    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
        dag=dag,
    )
    validate = PythonOperator(
        task_id='validate_loaded_data',
        python_callable=validate_loaded_data,
        dag=dag,
    )
    extract>>clean>>merge>>transform>>load>>validate
