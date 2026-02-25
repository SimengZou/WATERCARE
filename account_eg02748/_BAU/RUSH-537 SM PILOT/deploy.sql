desc task PRD_SM_PORTAL_TASK;
desc procedure PRD_WCC_DATAWAREHOUSE.DATAHUB_INTEGRATION.PRD_SM_PORTAL_S3();
desc stream DATAHUB_INTEGRATION.PRD_STREAM_SM_APSY_SMINF_CUR;
select get_ddl('task','PRD_SM_PORTAL_TASK');
select get_ddl('procedure','PRD_WCC_DATAWAREHOUSE.DATAHUB_INTEGRATION.PRD_SM_PORTAL_S3()');
select get_ddl('stream','DATAHUB_INTEGRATION.PRD_STREAM_SM_APSY_SMINF_CUR');
select get_ddl('view','DATAHUB_TARGET.VW_SM_TARGET_PORTAL_DIFFERENTIAL_S3');
select get_ddl('view','DATAHUB_INTEGRATION.VW_SM_STREAM_PORTAL_DIFFERENTIAL_S3');

show tasks;
select * from "PRD_WCC_DATAWAREHOUSE"."DATAHUB_TARGET"."VW_SM_TARGET_PORTAL_DIFFERENTIAL_S3" where M_METER_TIME like '%LG2100412B%'
;

alter task PRD_SM_PORTAL_TASK suspend;


show tables;
create or replace stream PRD_STREAM_SM_APSY_SMINF_CUR on table DATAHUB_TARGET.SM_APSY_SMINF_CUR append_only = true SHOW_INITIAL_ROWS = TRUE;

desc procedure PRD_WCC_DATAWAREHOUSE.DATAHUB_INTEGRATION.PRD_SM_PORTAL_S3();

select IPS_ACCOUNTNUMBER,M_METER_TIME,M_VALUE,IPS_CONSUMPTIONPERCENTAGE from DATAHUB_INTEGRATION.VW_SM_STREAM_PORTAL_DIFFERENTIAL_S3;
--
create or replace view DATAHUB_INTEGRATION.VW_SM_STREAM_PORTAL_DIFFERENTIAL_S3(
	IPS_ACCOUNTNUMBER,
	M_METER_TIME,
	M_VALUE,
	IPS_CONSUMPTIONPERCENTAGE
) as
          			SELECT 

			I.IPS_ACCOUNTNUMBER

			,concat(to_varchar(dateadd('m',-30,dateadd('ms',s.M_METER_TIME/1000,'1970-01-01')), 'YYYY-MM-DD"T"HH24:MI:SS"Z"') ,'#',s.M_UNIT_ID) AS M_METER_TIME

			,M_VALUE*IPS_CONSUMPTIONPERCENTAGE AS M_VALUE

			,IPS_CONSUMPTIONPERCENTAGE

			FROM 

			( SELECT DISTINCT

			  M_UNIT_ID

			  ,M_METER_TIME

			  ,M_VALUE

			 FROM 

			 DATAHUB_TARGET.SM_APSY_SMINF_CUR

			 WHERE M_RESOURCE_TYPE ='Water_Flow_Readings_30min'

			 ) S JOIN DATAHUB_TARGET.VW_IPS_SM_ACCOUNT_UNITID I ON I.IPS_UNITID=S.M_UNIT_ID   

			 INNER JOIN (select distinct SMARTMETERID from DATAHUB_EXTRACT.SM_APSY_SMINF_PILOT2) PILOT ON PILOT.SMARTMETERID=S.M_UNIT_ID  ;
--
ls @PRD_WCC_DATAWAREHOUSE.DATAHUB_INTEGRATION.PRD_STAGE_wsl_apsy_pr_sminf_portal_differential_s3;

select system$stream_has_data('PRD_WCC_DATAWAREHOUSE.DATAHUB_INTEGRATION.PRD_STREAM_SM_APSY_SMINF_CUR');
--
create or replace task SM_PORTAL_TASK
	schedule='USING CRON 0/45 * * * * Pacific/Auckland'
	when system$stream_has_data('PRD_WCC_DATAWAREHOUSE.DATAHUB_INTEGRATION.PRD_STREAM_SM_APSY_SMINF_CUR')
	as CALL PRD_WCC_DATAWAREHOUSE.DATAHUB_INTEGRATION.PRD_SM_PORTAL_S3();
 --
 CREATE OR REPLACE PROCEDURE "PRD_SM_PORTAL_S3"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  
                //  Variables
                  
                  var debug = "True";                                 // do we want debug messages?
                  var result = "";                                    // return status of this proc call
                  const process_name = Object.keys(this)[0];          // name of currently executing process
                  var debug_statement = "";                           // debug message statement
                  var number_of_rows_inserted = 0;                             // track number of rows we have inserted
                var number_of_rows_updated = 0;                             // track number of rows we have updated
                
              //  Step 1.

              //  Select MAX process ID for this process
                  
                  var log_sql_command;
                  log_sql_command = "select MAX(process_execution_id) as max_id from datahub_logging.etl_log_ingestion_process ";
                  log_sql_command += "where process_name = ''" + process_name + "''";
                  
                  var log_result_set = snowflake.execute({sqlText: log_sql_command});         // execute select from log table
                  log_result_set.next();                                                      // expecting only 1 row as this is a MAX() query
                  var process_execution_id = log_result_set.getColumnValue(1);                // and only 1 return value

                  //  Increment MAX process ID to log a new run of this process
                  
                  if(process_execution_id === undefined)                                      // test if return value is NULL - first execution of this process
                      {process_execution_id = 1;}
                  else
                      {process_execution_id = process_execution_id + 1;}
                  
                  //  log debug message
                  if( debug === "True" ) 
                      {
                      debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug(''" + process_name + "'',''Started process, new ID = " + process_execution_id.toString() + "'');";
                      snowflake.execute({sqlText: debug_statement}); 
                      }

              //  Step 2.

              //  Start execution - log start

                  log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process(''Insert'',''" + process_name + "'',''Running'',''Started process execution.'', " + process_execution_id.toString() + ",0,0);";
                  snowflake.execute({sqlText: log_sql_command});

                snowflake.execute( {sqlText: "begin transaction;"} );
                  try
                      {
                          

                      //  log debug message
                      if( debug === "True" ) 
                          {
                          debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug(''" + process_name + "'',''Begin Transaction'');";
                          snowflake.execute({sqlText: debug_statement}); 
                          }
                          
                          snowflake.execute( {sqlText: 
				`
				rm @PRD_WCC_DATAWAREHOUSE.DATAHUB_INTEGRATION.PRD_STAGE_wsl_apsy_pr_sminf_portal_differential_s3;
				`
				   } );
				
                          
                          
             snowflake.execute( {sqlText: 
				`copy into s3://wsl-apsy-pr-sminf-portal-differential-s3/
			FROM (SELECT  OBJECT_CONSTRUCT(''accountId'', IPS_ACCOUNTNUMBER, ''readingTime'', M_METER_TIME, ''readingValue'', M_VALUE,''consumptionPercentage'',IPS_CONSUMPTIONPERCENTAGE)   FROM (
			
		 select IPS_ACCOUNTNUMBER,M_METER_TIME,M_VALUE,IPS_CONSUMPTIONPERCENTAGE 
         from 
         DATAHUB_INTEGRATION.VW_SM_STREAM_PORTAL_DIFFERENTIAL_S3
			 
			 )
			 
			 )
			storage_integration = PRD_WSL_PORTAL_DIFFERENTIAL
			FILE_FORMAT = (TYPE = JSON COMPRESSION = NONE)
			OVERWRITE = TRUE
			encryption=(type=''AWS_SSE_S3'')
			MAX_FILE_SIZE=9999;
				
				`
				   } );
				  
				  
		            
            
         
        snowflake.execute ({sqlText: "commit"});

        log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process(''UpdateEnd'',''" + process_name + "'',''Success'',''Completed process execution.'', " + process_execution_id.toString() + "," + number_of_rows_inserted.toString()+ "," +  number_of_rows_updated.toString() + ");";
        snowflake.execute({sqlText: log_sql_command});

        result = "Success"; 

        //  log debug message
        if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug(''" + process_name + "'',''Commit Transaction'');";
            snowflake.execute({sqlText: debug_statement}); 
            }

        }       

        catch (err)  {
            snowflake.execute( {sqlText: "rollback;"} );
            return "Failed: " + err;   // Return a success/error indicator.
            }


  return result;
  ';
 --
 CREATE OR REPLACE PROCEDURE "SM_PORTAL_S3"()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
  
                //  Variables
                  
                  var debug = "True";                                 // do we want debug messages?
                  var result = "";                                    // return status of this proc call
                  const process_name = Object.keys(this)[0];          // name of currently executing process
                  var debug_statement = "";                           // debug message statement
                  var number_of_rows_inserted = 0;                             // track number of rows we have inserted
                var number_of_rows_updated = 0;                             // track number of rows we have updated
                
              //  Step 1.

              //  Select MAX process ID for this process
                  
                  var log_sql_command;
                  log_sql_command = "select MAX(process_execution_id) as max_id from datahub_logging.etl_log_ingestion_process ";
                  log_sql_command += "where process_name = ''" + process_name + "''";
                  
                  var log_result_set = snowflake.execute({sqlText: log_sql_command});         // execute select from log table
                  log_result_set.next();                                                      // expecting only 1 row as this is a MAX() query
                  var process_execution_id = log_result_set.getColumnValue(1);                // and only 1 return value

                  //  Increment MAX process ID to log a new run of this process
                  
                  if(process_execution_id === undefined)                                      // test if return value is NULL - first execution of this process
                      {process_execution_id = 1;}
                  else
                      {process_execution_id = process_execution_id + 1;}
                  
                  //  log debug message
                  if( debug === "True" ) 
                      {
                      debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug(''" + process_name + "'',''Started process, new ID = " + process_execution_id.toString() + "'');";
                      snowflake.execute({sqlText: debug_statement}); 
                      }

              //  Step 2.

              //  Start execution - log start

                  log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process(''Insert'',''" + process_name + "'',''Running'',''Started process execution.'', " + process_execution_id.toString() + ",0,0);";
                  snowflake.execute({sqlText: log_sql_command});

                snowflake.execute( {sqlText: "begin transaction;"} );
                  try
                      {
                          

                      //  log debug message
                      if( debug === "True" ) 
                          {
                          debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug(''" + process_name + "'',''Begin Transaction'');";
                          snowflake.execute({sqlText: debug_statement}); 
                          }
                          
                          snowflake.execute( {sqlText: 
				`
				rm @PRD_WCC_DATAWAREHOUSE.DATAHUB_INTEGRATION.PRD_STAGE_wsl_apsy_pr_sminf_portal_differential_s3;
				`
				   } );
				
                          
                          
             snowflake.execute( {sqlText: 
				`copy into s3://wsl-apsy-pr-sminf-portal-differential-s3/
			FROM (SELECT  OBJECT_CONSTRUCT(''accountId'', IPS_ACCOUNTNUMBER, ''readingTime'', M_METER_TIME, ''readingValue'', M_VALUE,''consumptionPercentage'',IPS_CONSUMPTIONPERCENTAGE)   FROM (
			
	           select IPS_ACCOUNTNUMBER,M_METER_TIME,M_VALUE,IPS_CONSUMPTIONPERCENTAGE from DATAHUB_INTEGRATION.VW_SM_STREAM_PORTAL_DIFFERENTIAL_S3
			 
			 )
			 
			 )
			storage_integration = PRD_WSL_PORTAL_DIFFERENTIAL
			FILE_FORMAT = (TYPE = JSON COMPRESSION = NONE)
			OVERWRITE = TRUE
			encryption=(type=''AWS_SSE_S3'')
			MAX_FILE_SIZE=9999;
				
				`
				   } );
				  
        snowflake.execute ({sqlText: "commit"});

        log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process(''UpdateEnd'',''" + process_name + "'',''Success'',''Completed process execution.'', " + process_execution_id.toString() + "," + number_of_rows_inserted.toString()+ "," +  number_of_rows_updated.toString() + ");";
        snowflake.execute({sqlText: log_sql_command});

        result = "Success"; 

        //  log debug message
        if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug(''" + process_name + "'',''Commit Transaction'');";
            snowflake.execute({sqlText: debug_statement}); 
            }

        }       

        catch (err)  {
            snowflake.execute( {sqlText: "rollback;"} );
            return "Failed: " + err;   // Return a success/error indicator.
            }


  return result;
  ';