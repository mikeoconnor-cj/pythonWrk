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
import pprint
import pyinputplus as pyip

tableKeysStruct = {}

tableType = ''
orgDB = ''

tableType = pyip.inputMenu(['Not Network Not Layup','Network Not Layup','Layup'], lettered=False, numbered=True)

orgDB = pyip.inputMenu(['INT_BEAUMONT', 'INT_BRIGHT', 'INT_CHENMED', 'INT_CLOVERNA', 'INT_EVOLENT', 'INT_OSCAR', 'INT_PARADIGM', 'INT_PRIVIA'], lettered=False, numbered=True)



try:
    sf.paramstyle = 'qmark'
    sf_conn = sf.connect(
        # user=user, # Snowflake user e.g. first_name.last_name@carejourney.com
        user='michael.oconnor@carejourney.com',
        # password=password, # carejourney-prod Okta password
        password='W1xk3dY007&1P@',
        account='carejourney_nci.us-east-1',
        authenticator='externalbrowser',
        warehouse='DEV_BRIGHT', # snowflake warehouse --optional
        database='DEV_BRIGHT' # snowflake db --optional
    )

    sf_cursor = sf_conn.cursor()

    sql = ''
    tableWoutPK = []

    # need access to data_model db in nci
    if tableType == 'Not Network Not Layup': #? base 48
        sql = 'SELECT table_name FROM data_model.information_schema.TABLES WHERE table_schema LIKE \'VRDC%\' AND TABLE_name NOT LIKE \'%NETWORK%\' AND table_name NOT LIKE \'%LAYUP%\' order by table_name'
    elif tableType == 'Network Not Layup': # 43
        sql = 'SELECT table_name FROM data_model.information_schema.TABLES WHERE table_schema LIKE \'VRDC%\' AND TABLE_name LIKE \'%NETWORK%\' AND table_name NOT LIKE \'%LAYUP%\' order by table_name'    
    else:  # layup 24 includes network_layup
        sql = 'SELECT table_name FROM data_model.information_schema.TABLES WHERE table_schema LIKE \'VRDC%\' AND TABLE_name LIKE \'%LAYUP%\' order by table_name'
 
    sf_cursor.execute(sql)
    df = sf_cursor.fetch_pandas_all()
    singleColTN=df.loc[:,'TABLE_NAME']

    # a list of all tables in the schema, there's 115 in vrdc in total
    tblList = singleColTN.tolist()

    # test with the first 15 tables
    for i in range(len(tblList)):
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
            tableWoutPK.append(tblList[i])

    pprint.pprint(tableKeysStruct)

    # tableKeys = tableKeysStruct.keys()
    # df3 = pd.DataFrame(tableKeys, columns=["table_name"])
        # overwrites if exists
    # df3.to_excel('/Users/michael.oconnor/downloads/myTables3.xlsx',sheet_name=tableType)

    print('tables w/out pks: ', tableWoutPK)

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
