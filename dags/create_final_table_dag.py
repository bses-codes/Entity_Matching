from datetime import datetime, timedelta
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from function_dag import *


def create_final_table():
    layout_1, layout_2, layout_3, layout_4, layout_5 = read_data()

    # store lengths of dataframes
    store(layout_1, layout_2, layout_3, layout_4, layout_5)

    # created combined column for each layout
    df_lay1 = combine_columns(layout_1.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
    df_lay2 = combine_columns(layout_2.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
    df_lay3 = combine_columns(layout_3.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
    df_lay4 = combine_columns(layout_4.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
    df_lay5 = combine_columns(layout_5.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')

    # merge layout 1 and layout 2
    c_df1 = combine(layout_1, layout_2, df_lay1, df_lay2, 'Customer Code', 'Mobile Number')
    c_df1 = merge_columns(c_df1.copy())

    # merge layout 3
    df_c_df1 = combine_columns(c_df1.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
    c_df2 = combine(c_df1, layout_3, df_c_df1, df_lay3, 'Customer Code', 'votersID')
    c_df2 = merge_columns(c_df2.copy())

    # merge layout 4
    df_c_df2 = combine_columns(c_df2.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
    c_df3 = combine(c_df2, layout_4, df_c_df2, df_lay4, 'Customer Code', 'Electricity Bill ID')
    c_df3 = merge_columns(c_df3.copy())

    # merge layout 5
    df_c_df3 = combine_columns(c_df3.copy(), ['Name', 'Father Name', 'Date of Birth'], 'combined')
    c_df4 = combine(c_df3, layout_5, df_c_df3, df_lay5, 'Customer Code', 'License Number')
    c_df4 = merge_columns(c_df4.copy())

    # create final table

    c_df4['Updated Date'] = datetime.today().strftime('%Y-%m-%d')
    # create unique universal id for each record
    final_df = generate_unique_id(c_df4.copy())

    # save to a csv file
    final_df.to_csv('/opt/airflow/datasets/final_table.csv', index=False)
    # engine = create_engine("mysql+pymysql://<YOUR_DB_USERNAME>:<YOUR_DB_PASSWORD>@host.docker.internal/<YOUR_DB_NAME>")
    # final_df.to_sql('<YOUR_TABLE_NAME>', engine, if_exists='append', index=False)
default_args = {
    'owner': 'bses',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_final_table',
    default_args=default_args,
    start_date=datetime(2024, 5, 29),
    schedule_interval='@once'
) as dag:
    final_table = PythonOperator(
        task_id='create_final_table',
        python_callable=create_final_table
    )


    final_table