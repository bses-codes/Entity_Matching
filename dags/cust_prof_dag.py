import mysql.connector as connection
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
def con():

    conn = connection.connect(
        host="host.docker.internal",
        database = 'test',
        user='root',
        password='12345678')

    query = "Select * from rw_transaction_data"
    rw_transaction_data = pd.read_sql(query,conn)
    query = "Select * from  product_category_map"
    product_category_map = pd.read_sql(query,conn)
    query = "Select * from product_category"
    product_category = pd.read_sql(query,conn)
    query = "Select * from customer_profile"
    customer_profile = pd.read_sql(query,conn)
    query = "Select * from products"
    products = pd.read_sql(query,conn)
    merged_df = pd.merge(rw_transaction_data, product_category_map, on=['module_id', 'product_id', 'product_type_id'], how='inner')

    return merged_df
def analysis(ti):
    merged_df = ti.xcom_pull(task_ids='connect', key='return_value')
    merged_df['last_modified_date'] = pd.to_datetime(merged_df['last_modified_date'])
    merged_df['month'] = merged_df['last_modified_date'].dt.month
    grouped_data = merged_df.groupby(['payer_account_id', 'month', 'txn_flow'])
    final_df = merged_df.groupby('payer_account_id')['reward_point'].sum()

    sum_amount_by_month = grouped_data['amount'].sum()

    pivot_table = merged_df.pivot_table(index='payer_account_id', columns='txn_flow', values='amount', aggfunc='sum')
    pivot_table.reset_index(inplace=True)
    pivot_table.fillna(0, inplace=True)
    pivot_table.columns = ['payer_account_id', 'Total InFlow Amount', 'Total OutFlow Amount','Total Valuechain Amount']
    df1 = pd.DataFrame(pivot_table)
    final_df = pd.merge(final_df,df1,on='payer_account_id')

    pivot_table1 = merged_df.pivot_table(index='payer_account_id', columns='txn_flow', values='amount', aggfunc='count')
    pivot_table1.reset_index(inplace=True)
    pivot_table1.fillna(0, inplace=True)
    pivot_table1.columns = ['payer_account_id', 'Total InFlow Count', 'Total OutFlow Count','Total Valuechain Count']
    final_df = pd.merge(final_df,pd.DataFrame(pivot_table1),on='payer_account_id')

    df = pd.DataFrame(sum_amount_by_month)
    pivot_table = df.pivot_table(index='payer_account_id', columns='txn_flow', values='amount', aggfunc='mean')
    pivot_table.reset_index(inplace=True)
    pivot_table.fillna(0, inplace=True)
    pivot_table.columns = ['payer_account_id','monthly_inflow_amount', 'monthly_outflow_amount', 'monthly_valuechain_amount']
    df1 = pd.DataFrame(pivot_table)
    final_df = pd.merge(final_df,df1,on='payer_account_id')

    count_pivot_table = pd.pivot_table(merged_df, values='amount', index=['payer_account_id', 'month'], columns='txn_flow', aggfunc='count')
    average_per_account = count_pivot_table.groupby('payer_account_id').mean()
    average_per_account.rename(columns={'InFlow': 'Average InFlow', 'OutFlow': 'Average OutFlow', 'ValueChain': 'Average ValueChain'}, inplace=True)
    data = pd.DataFrame(average_per_account)
    final_df = pd.merge(final_df,data,on='payer_account_id')

    sorted_df = merged_df.sort_values(by=['last_modified_date', 'time'], ascending=False)
    latest_product_per_account = sorted_df.groupby('payer_account_id').first()
    latest_product_per_account = latest_product_per_account[['product_name', 'last_modified_date']]
    latest_product_per_account.reset_index(inplace=True)
    df3 = pd.DataFrame(latest_product_per_account)
    df3.columns = ['payer_account_id','latest_product_used','latest_tran_date']
    final_df = pd.merge(final_df,df3,on='payer_account_id')

    merged_df['last_modified_date'] = pd.to_datetime(merged_df['last_modified_date'])
    merged_df['month'] = merged_df['last_modified_date'].dt.month
    monthly_revenue = merged_df.groupby(['payer_account_id', 'month'])['revenue_amount'].sum()
    average_monthly_revenue_per_account = monthly_revenue.groupby('payer_account_id').mean()
    df4 = pd.DataFrame(average_monthly_revenue_per_account)
    df4.columns = ['monthly_average_lifetime_revenue']
    final_df = pd.merge(final_df,df4,on='payer_account_id')

    df6 = merged_df.groupby('payer_account_id')['revenue_amount'].sum()
    df6.columns = ['total_revenue']
    final_df = pd.merge(final_df,df6,on='payer_account_id')

    df7 = merged_df.groupby('payer_account_id')['product_name'].nunique()
    df7 = pd.DataFrame(df7).reset_index()
    df7.columns = ['payer_account_id', 'product_usage']
    final_df = pd.merge(final_df,df7,on='payer_account_id')

    product_counts = merged_df.groupby(['payer_account_id', 'product_name']).size().reset_index(name='count')
    product_counts_sorted = product_counts.sort_values(by=['payer_account_id', 'count'], ascending=[True, False])
    top_three_products = product_counts_sorted.groupby('payer_account_id').head(3)
    pivot_df = top_three_products.pivot_table(index='payer_account_id', columns=top_three_products.groupby('payer_account_id').cumcount() + 1, values='product_name', aggfunc='first')
    pivot_df.columns = ['most_used_product', 'second_most_used_product', 'thirdmost_used_product']
    pivot_df.reset_index(inplace=True)
    df8 = pd.DataFrame(pivot_df)

    final_df = pd.merge(final_df,df8,on='payer_account_id')
    final_df['run_date'] = datetime.today().strftime('%Y-%m-%d')

    final_df.to_csv('/opt/airflow/dags/final.csv')


default_args = {
    'owner': 'bses',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='dag_analysis',
    default_args=default_args,
    start_date=datetime(2024, 5, 1),
    schedule_interval='@daily'
) as dag:
    connect = PythonOperator(
        task_id='connect',
        python_callable=con
    )
    analysis = PythonOperator(
        task_id='analysis',
        python_callable=analysis,
    )

    connect >> analysis