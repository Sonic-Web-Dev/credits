import boto3
from boto3.dynamodb.conditions import Key, Attr
import pickle

import numpy as np
import pandas as pd
import os
import time as t

import datetime
from datetime import datetime, time, timedelta 

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import UUID

from collections import OrderedDict

s3 = boto3.resource('s3')
conn_string = os.getenv('url')
db = create_engine(conn_string)
conn = db.connect()

df = pd.DataFrame(columns=["record_modified","agent_id","other_agent_id","action_id", "transfer_type", "transfer_amount"])
df_agent = pd.DataFrame(columns=["agent_id", "corpID"])
df_history = pd.DataFrame(columns=["agent_id","action_id","date","amount","total","origin_id", "type", "destination_id"])
df_starting_balance = pd.DataFrame()

#pd.set_option('display.max_columns', None)  # or 1000
#pd.set_option('display.max_rows', None)  # or 1000
#pd.set_option('display.max_colwidth', None)  # or 199

def sort_trans(row, d):
    # print(row['record_modified'])
    od = OrderedDict() 
    if row['transfer_type'] == 'Inbound Transfer':
        if row['agent_id'] in d:
            od = d[row['agent_id']]
        if row['agent_id'] + ":temp" in d:
            count = 0
            test = (d[row['agent_id'] + ":temp"])
            for key, value in test.items():
                od[row['action_id'] + ':' + str(count)] = {'deposit_id': key,'amount': value, 'datetime': row['record_modified'], 'agent_id': row['agent_id'], 'action_id': row['action_id'] + ':' + str(count)}
                df_history.loc[len(df_history)] = [row['agent_id'],row['action_id'] + ':' + str(count), row['record_modified'], value, row['transfer_amount'], key,"Inbound Transfer", row['agent_id']]
                count = count + 1
            del d[row['agent_id'] + ":temp"] 
        else:
            try:
                print(d[row['agent_id'] + ":temp"])
            except Exception as e:
                print(row)
                print("Exception" + e)
            od[row['action_id']] = {'deposit_id': row['other_agent_id'],'amount': row['transfer_amount'], 'datetime': row['record_modified'],'agent_id': row['agent_id'],'action_id': row['action_id']}
        d[row['agent_id']] = od
    elif row['transfer_type'] == 'Deposit':
        if row['agent_id'] in d:
            od = d[row['agent_id']]
        od[row['action_id']] = {'deposit_id': row['agent_id'] + '-stripe_ID','amount': row['transfer_amount'], 'datetime': row['record_modified'], 'agent_id': row['agent_id'],'action_id': row['action_id']}
        df_history.loc[len(df_history)] = [row['agent_id'],row['action_id'], row['record_modified'], row['transfer_amount'], row['transfer_amount'], row['agent_id'] + '-stripe_ID',"Deposit", row['agent_id']]
        d[row['agent_id']] = od
    elif row['transfer_type'] == 'Outbound Transfer' or row['transfer_type'] == 'Lead Purchase':
        handshake = OrderedDict()
        for value in d[row['agent_id']]:
            pre_amount = d[row['agent_id']][value]['amount']
            if pre_amount > 0:  
                transfer_amount = abs(row['transfer_amount'])
                if row['transfer_amount'] == 0:
                    break
                if pre_amount >= transfer_amount:
                    pre_amount -= transfer_amount
                    if row['transfer_type'] == 'Outbound Transfer':
                        df_history.loc[len(df_history)] = [row['agent_id'],value, row['record_modified'], -1 * transfer_amount,pre_amount, d[row['agent_id']][value]['deposit_id'], "Outbound Transfer", row['other_agent_id']]
                        handshake[d[row['agent_id']][value]['deposit_id']] = transfer_amount
                    elif row['transfer_type'] == 'Lead Purchase':
                        df_history.loc[len(df_history)] = [row['agent_id'],value, row['record_modified'], -1 * transfer_amount,pre_amount, d[row['agent_id']][value]['deposit_id'],"Lead Purchase", row['action_id']]                                        
                    transfer_amount = 0
                elif transfer_amount > pre_amount:
                    if row['transfer_type'] == 'Outbound Transfer':
                        df_history.loc[len(df_history)] = [row['agent_id'],value, row['record_modified'], -1 * pre_amount, 0, d[row['agent_id']][value]['deposit_id'], "Outbound Transfer", row['other_agent_id']]
                        handshake[d[row['agent_id']][value]['deposit_id']] = pre_amount
                    elif row['transfer_type'] == 'Lead Purchase':
                        df_history.loc[len(df_history)] = [row['agent_id'],value, row['record_modified'], -1 * pre_amount,0, d[row['agent_id']][value]['deposit_id'], "Lead Purchase", row['action_id']]                                                                   
                    transfer_amount -= pre_amount
                    pre_amount = 0
                row['transfer_amount'] = transfer_amount
                d[row['agent_id']][value]['amount'] = pre_amount
            else:
                continue
        if row['other_agent_id'] != "N/a":
            d[row['other_agent_id'] + ":temp"] = handshake      

def network_scan(dynamodb=None ):
    networkIDs = []
    rootAgents = []
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb',region_name='us-east-1')

    table = dynamodb.Table('networks-prod')
    response = table.scan(
        FilterExpression=Attr('legacySync').eq(True),
        )
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression=Attr('legacySync').eq(True),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
    
    for index ,row in enumerate(data):
        networkIDs.append(row['networkID'])
        rootAgents.append(row['brokerID'])

    return networkIDs, rootAgents
        


def agent_scan(networkID, dynamodb=None ):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb',region_name='us-east-1')

    table = dynamodb.Table('agents-prod')
    response = table.scan(
        FilterExpression=Attr('networkID').eq(networkID),
        )
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        t.sleep(5)
        response = table.scan(
            FilterExpression=Attr('networkID').eq(networkID),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])
    
    for index ,row in enumerate(data):
        try:
            df_agent.loc[len(df_agent)] = [row['agentID'], row['corpID']]
        except:
            df_agent.loc[len(df_agent)] = [row['agentID'], 'Unknown']
    

def transfer_query(num,currentDate,agentID, dynamodb=None):
    print(num + ": " + agentID)
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb',region_name='us-east-1')
    
    table = dynamodb.Table('agent_activity-prod')

    response = table.query(
        KeyConditionExpression=Key('agentID').eq(agentID),
        FilterExpression=Attr('activityDate').between(str(currentDate),datetime(2021, 10, 21,7,1).strftime("%Y-%m-%dT%H:%M:%SZ")) & (Attr("source").eq("credit_record") | Attr("source").eq("campaign_record")),
    )
    data = response['Items']

    # datetime.today().strftime("%Y-%m-%dT%H:%m:%SZ")

    while 'LastEvaluatedKey' in response:
        t.sleep(0.1)
        response = table.query(
        KeyConditionExpression=Key('agentID').eq(agentID),
        FilterExpression=Attr('activityDate').between(str(currentDate),datetime(2021, 10, 21,7,1).strftime("%Y-%m-%dT%H:%M:%SZ")) &  (Attr("source").eq("credit_record") | Attr("source").eq("campaign_record")),
        ExclusiveStartKey=response['LastEvaluatedKey']
        )
        data.extend(response['Items'])

    for row in enumerate(data):
        amount = float(row[1]['changedItems']['credit']['newItem']) - float(row[1]['changedItems']['credit']['oldItem'])
        if ('fundType' in row[1] and row[1]['fundType'] == 'MANUAL') or ('fundType' in row[1] and row[1]['fundType'] == 'AUTO'):
            df.loc[len(df)] = [row[1]['activityDate'],row[1]["agentID"],"N/a",row[1]["actionID"],"Deposit", amount]
        elif (amount > 0) & (row[1]['source'] == 'credit_record'):
            df.loc[len(df)] = [row[1]['activityDate'],row[1]["agentID"],row[1]["otherAgentID"],row[1]["actionID"],"Inbound Transfer", amount]
        elif (amount < 0) & (row[1]['source'] == 'credit_record'):
            df.loc[len(df)] = [row[1]['activityDate'],row[1]["agentID"],row[1]["otherAgentID"],row[1]["actionID"],"Outbound Transfer", amount]
        elif row[1]['source'] == 'campaign_record':
            df.loc[len(df)] = [row[1]['activityDate'],row[1]["agentID"],"N/a",row[1]["leadID"],"Lead Purchase", amount]


def handler():
    #print("Received event: " + json.dumps(event, indent=2))

    #Get the object from the event and show its content type
    try:

        array = []
        bucket='atcu-prod'
        key='creditsLookback/data.p'
        print(key)
        networkIDs, rootAgents = network_scan()
        for network in networkIDs:
            agent_scan(network)
        print(len(df_agent))
        rs = conn.execute('SELECT record_modified FROM agent_activities_prod WHERE record_modified IN (SELECT max(record_modified) FROM agent_activities_prod)')
        for row in rs:
            currentDate = row[0]
            print(currentDate)
        for i in range(0,len(df_agent)):
            # transfer_query(str(len(array) + 1),currentDate,df_agent.iloc[i]['agent_id'])
            array.append(df_agent.iloc[i]['agent_id'])
            # if len(df) > 10000:
            #     df.to_sql("agent_activities_prod", con=conn, if_exists='append', index=False)
            #     df.drop(df.index, inplace=True)
            #     print(df)

        rs = conn.execute('SELECT date FROM history_prod WHERE date IN (SELECT max(date) FROM history_prod);')
        for row in rs:
            print(row)
            currentDateInHistoryTable = row[0]
        print(currentDateInHistoryTable)
        for sql_agent in pd.read_sql("SELECT record_modified, left(record_modified,-3) as modified,agent_id, other_agent_id,action_id, transfer_amount, transfer_type FROM agent_activities_prod WHERE record_modified > '" + currentDateInHistoryTable + "' ORDER BY modified ASC, case transfer_type when 'Lead Purchase' then 1 when 'Deposit' then 2 when 'Outbound Transfer' then 4 when 'Inbound Transfer' then 3 else 0 end DESC;", conn, chunksize=10000):
            d = pickle.loads(s3.Bucket(bucket).Object(key).get()['Body'].read())
            sql_agent.apply(lambda x: sort_trans(x, d), axis=1)
            try:
                rs = conn.execute('DROP TABLE agent_buckets;')
            except:
                print("Table Doesn't Exist")
            for agent in array:
                try:
                    df1 = pd.DataFrame(d[agent], columns=d[agent].keys())
                    df1.T.to_sql("agent_buckets", con=conn, if_exists='append', index=False)
                except:
                    continue

            df_history.to_sql("history_prod", con=conn, if_exists='append', index=False)
            df_history.drop(df_history.index, inplace=True)
            pickle_byte_obj = pickle.dumps(d) 
            s3.Object(bucket,key).put(Body=pickle_byte_obj)

        return "Pass"

    except Exception as e:
        print(e)
        raise e

key = os.getenv('string')
print(key)
handler()