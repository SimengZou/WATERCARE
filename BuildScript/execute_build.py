import csv
import os
import glob
import shutil
import datetime
import snowflake.connector
import requests
import json
import sys
import re

def build_stage(schema, file):
    file=file.replace('\\', '/').replace('\n', '')
    print('file :' + file + ' schema :' + schema)

    cursor = conn.cursor()
    cursor.execute('use schema ' + schema)
    cursor.close()

    if '*' in file:
        dirpath=file.replace('*', '')
        filelist = os.listdir(dirpath)
        for file_name in filelist:
            with open(os.environ.get('local_directory') + '/' + dirpath + '/' + file_name, 'r') as filereader:
                sql=convertvariable(filereader.read())
                sqllist=sql.split('alter ')
                index=0

                for sqlsplit in sqllist:
                    if index == 0:
                        runsql(sqlsplit)
                    else:                        
                        runsql('alter ' + sqlsplit)
                    index=1
    else:
        with open(os.environ.get('local_directory') + '/' + file, 'r') as filereader:
            sql=convertvariable(filereader.read())
            sqllist=sql.split('alter ')
            index=0

            for sqlsplit in sqllist:
                if index == 0:
                    runsql(sqlsplit)
                else:                        
                    runsql('alter ' + sqlsplit)
                index=1

def convertvariable(sql):

    env_lower='dev'
    pattern = r'\${(.+?)}'
    match = re.search(pattern, sql)

    for var_name in re.findall(pattern, sql):
        var_name_after_dot=var_name.split('.')[1]
    
        with open(os.environ.get('local_directory') + '/BuildScript/Var/' + os.environ.get('environment').lower() + '.json', 'r') as varreader:
            for varline in varreader:
                varline=varline.replace('\"', '').replace(' ', '')

                if varline.startswith(var_name_after_dot):
                    # process the line
                    index=0
                    formated_varline=''
                    for value in varline.split(':'):
                        if index == 1:
                            formated_varline=value.replace(',\n', '').replace('\n', '')
                            index=2
                        elif index == 2 :
                            formated_varline=formated_varline+':'+value.replace(',\n', '').replace('\n', '')
                        else:
                            index=1
                    sql=sql.replace('${' + var_name + '}', formated_varline)
    return sql

def runsql(sql):
    # Execute SQL
    cursor = conn.cursor()
    cursor.execute(sql)

    # Close cursor to Snowflake
    cursor.close()

build_file = os.environ.get('local_directory') + '/BuildScript/Builds/' + os.environ.get('branch').removeprefix('refs/heads/') + '.build'

#if (os.environ.get('environment') == 'PRD' or os.environ.get('environment') == 'STG') and not(os.environ.get('branch') == 'master'):
#    print('Only the master can be execute in STG and PRD')
#    sys.exit(6)

conn = snowflake.connector.connect(
    user=os.environ.get('snowflake_deployment_user'),
    password=os.getenv('snowflake_deployment_pw'),
    account='eg02748',
    region='ap-southeast-2',
    database=os.environ.get('environment') + '_WCC_DATAWAREHOUSE',
    role='ROLE_' + os.environ.get('environment').upper() + '_DEPLOYMENT',
    warehouse='WH_DEPLOYMENT'
)

with open(build_file, 'r') as buildreader:
    for buildline in buildreader:
        remove_comment = buildline.split('#')
        build_line_without_comment = remove_comment[0]
        if not(build_line_without_comment.isspace() or len(build_line_without_comment) == 0):
            build_stage(build_line_without_comment.split(' ')[0], build_line_without_comment.split(' ')[1])
# Close connection to Snowflake
conn.close()