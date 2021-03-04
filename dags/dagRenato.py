# import necessary libraries
from datetime import timedelta
from datetime import datetime
import json
import pandas as pd
import os

import airflow
from airflow.models import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator


# Airflow variables for the data files
jsonFile = Variable.get('raw_json_data', '/tmp/files/meetup.json')
# csvFile = Variable.get('parsed_data', '/tmp/files/outTest.csv')
csvFile = Variable.get('parsed_data', '/tmp/files/parsedData.csv')

# Airflow variavble to use as line counter
lineNumber = Variable.get('count', default_var=0)


def readJsonData(jsonData, csvOut, lineNum):
    """    
    Read a json, parse each line and save it to csv

    Arguments:
        jsonData {json file} -- file containing the data to be parsed
        csvOut {csv file} -- will be created and the parsed data will be added to it
        lineNum {number in str format} -- json line counter
    """

    # open json file
    with open(jsonData) as dataFile:
        # try except error handler
        try:
            # read each line
            line = dataFile.readlines()[int(lineNum)].strip()

            # dict for parsed data
            parsedline = {}
            # transform line to dataframe and drop/remove columns not needed
            line = json.loads(line)
            # make sure the meets are in phisical locations and are public
            if 'venue' in line.keys():
                # get the time of the event and convert it from timestamp to datetime
                try:
                    eventTime = datetime.fromtimestamp(line['event']['time'])\
                        .strftime('%Y-%m-%d %H:%M')
                except ValueError:
                    # if the timestamp is in miliseconds, convert to seconds
                    eventTime = datetime.fromtimestamp(line['event']['time']/1000)\
                        .strftime('%Y-%m-%d %H:%M')
                eventTime = datetime.strptime(eventTime, '%Y-%m-%d %H:%M')
                # get the date and hour
                parsedline['event_day'] = eventTime.date().strftime('%Y-%m-%d')
                parsedline['event_hour'] = eventTime.time().strftime('%H:%M')
                # get the topics
                parsedline['topics'] = [topicDict['topic_name']
                                        for topicDict in line['group']['group_topics']]
                # get the city
                parsedline['city'] = line['group']['group_city']
                # get the cou6ntry
                parsedline['country'] = line['group']['group_country']

                # save the parsed data to a dataframe
                df = pd.DataFrame(parsedline)

                # create the csv, add the headers and the first line
                if not os.path.exists(csvOut):
                    df.to_csv(csvOut,
                              mode='a',
                              encoding='utf8',
                              index=False)
                else:
                    # add line, without the headers, to the csv
                    df.to_csv(csvOut,
                              mode='a',
                              encoding='utf8',
                              index=False,
                              header=False)

        except:
            # pass over broken data or if the counter exceeds line numbers
            pass

        # increment the line counter
        lineNumber = Variable.set('count', int(lineNum)+1)


# set up DAG arguments
args = {
    'owner': 'Renato_Otescu',
    'start_date': datetime(2020, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(seconds=30)
}

# set up DAG to be executed
dag = DAG(
    dag_id='analyze_json_data',
    default_args=args,
    catchup=False,
    schedule_interval='*/1 * * * *',
    # schedule_interval=timedelta(minutes=1),
    dagrun_timeout=timedelta(minutes=60),
)

# dummy start task
start = DummyOperator(
    task_id='start',
    dag=dag
)

# call the python function and pass it's arguments
parseJsonFile = PythonOperator(
    task_id='parse_json',
    python_callable=readJsonData,
    op_args=(jsonFile, csvFile, lineNumber),
    dag=dag
)

# execute the Spark script
getTopics = BashOperator(
    task_id='get_topics',
    bash_command='python /tmp/files/sparkRenato.py',
    dag=dag,
)

# dummy end task
end = DummyOperator(
    task_id='end',
    dag=dag
)

# set tasks relations (upstream/downstream)
start >> parseJsonFile
parseJsonFile >> getTopics
end << getTopics

# script can be executed from the command line
if __name__ == "__main__":
    dag.cli()
