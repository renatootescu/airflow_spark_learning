# Details
The files are organized based on the instructions and the directory structure in the Docker container here: https://github.com/nielsdenissen/container-spark-airflow

## Airflow DAG
The file **dagRenato.py**, in the **dags** directory, should pe placed in the **dags/** directory in the container.

## Spark script
The file **sparkRenato.py**, in the **files** directory, should pe placed in the **files/** directory in the container.

## CSV files
The 2 csv files in the **files** directory, **parsedData.csv** and **topTopics.csv**, are the end result after running the Airflow job for all the meetup RSVPs. They are saved in the **files/** directory in the container.

**Thank you!**

**Renato**