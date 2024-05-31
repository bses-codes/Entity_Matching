## Steps to run entity matching

1. Download **Docker Desktop**.
2. Open the folder entity using preferred editor or IDE.
3. Create a Virtual Environment.
4. Install requirements.txt using `pip install -r requirements.txt`
5. On terminal, execute the commands :
   * `docker build -t extended-airflow-image`
   * `docker-compose up`
6. Wait for airflow webserver to start and login using username airflow and password airflow or anything you set up.
7. Trigger the dag **dag_final_table**. This will create final.csv file in the datasets folder.
8. If you add anymore data in any of the layouts, trigger the dag named **dag_update_table**.
   
To store and retrieve data from mysql database update the following strings to actual value. 
* <YOUR_DB_NAME>
* <YOUR_TABLE_NAME>
* <YOUR_DB_USERNAME>
* <YOUR_DB_PASSOWRD>



