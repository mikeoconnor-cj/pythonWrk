#! python3

# dupCheckScriptGen.py

# Use datastructures like a dictionary to load data that can be used for a dup checker
# use the snowflake connector to load this data and generate and execute correct group-by 
# dup checker scripts

import snowflake.connector as sf
# had to pip3 install --user snowflake-connector-python[pandas]
# and then path to the location (I don't use anaconda).. part of below doc helped
# https://navhealth.atlassian.net/wiki/spaces/RES/pages/2224095370/Setup+Python3+and+Anaconda+on+Mac

import pandas as pd
from tabulate import tabulate

tableKeysStruct = {}


try:
    sf.paramstyle = 'qmark'
    sf_conn = sf.connect(
        # user=user, # Snowflake user e.g. first_name.last_name@carejourney.com
        user='michael.oconnor@carejourney.com',
        # password=password, # carejourney-prod Okta password
        password='MYPASSWORD',
        account='carejourney_nci.us-east-1',
        authenticator='externalbrowser',
        warehouse='DEV_BRIGHT', # snowflake warehouse --optional
        database='DEV_BRIGHT' # snowflake db --optional
    )

    sf_cursor = sf_conn.cursor()

    # need access to data_model db in nci
    sf_cursor.execute('SELECT table_name FROM data_model.information_schema.TABLES WHERE table_schema = \'VRDC\' order by table_name')
    df = sf_cursor.fetch_pandas_all()
    singleColTN=df.loc[:,'TABLE_NAME']

    # a list of all tables in the schema, there's 115 in vrdc
    tblList = singleColTN.tolist()

    # test with the first 15 tables
    for i in range(len(tblList)-100):
        # need access to data_model.vrdc schema
        sqlString = 'SHOW PRIMARY KEYS IN TABLE DATA_MODEL.VRDC.' + tblList[i]
        sf_cursor.execute(sqlString)
        table_results = []
        results = sf_cursor.fetchall()  # df = sf_cursor.fetch_pandas_all()  throws an 'unknown' error ? because no column names? 

        # print(str(len(results)))
        for idx in range(len(results)):
            table_results.append([results[idx][3], results[idx][4]])

        print(tabulate(table_results, headers=["table_name", "pk_name"], tablefmt='psql'))
        df2 = pd.DataFrame(table_results, columns=["table_name", "pk_name"])
        singleColPK = df2.loc[:,"pk_name"]
        tblPKList = singleColPK.tolist()
        # can I create the pk list without pandas above?

        # load the dictionary
        if len(tblPKList) > 0:
            tableKeysStruct[tblList[i]] = tblPKList
        else:
            print(tblList[i] + ' has no keys')

    print(tableKeysStruct)

except Exception as e:
    print(e)

finally:
    try:
        sf_cursor.close()
    except:
        pass
    try:
        sf_conn.close()
    except:
        pass
