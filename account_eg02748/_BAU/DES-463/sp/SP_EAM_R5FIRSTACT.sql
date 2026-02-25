
                    
                create or replace procedure DATAHUB_INTEGRATION.EAM_TARGET_MERGE_R5FIRSTACT()
                  returns varchar not null
                  language javascript
                  as
                  $$
  
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
                  log_sql_command += "where process_name = '" + process_name + "'";
                  
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
                      debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug('" + process_name + "','Started process, new ID = " + process_execution_id.toString() + "');";
                      snowflake.execute({sqlText: debug_statement}); 
                      }

              //  Step 2.

              //  Start execution - log start

                  log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('Insert','" + process_name + "','Running','Started process execution.', " + process_execution_id.toString() + ",0,0);";
                  snowflake.execute({sqlText: log_sql_command});

                snowflake.execute( {sqlText: "begin transaction;"} );
                  try
                      {
                          

                      //  log debug message
                      if( debug === "True" ) 
                          {
                          debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug('" + process_name + "','Begin Transaction');";
                          snowflake.execute({sqlText: debug_statement}); 
                          }      
                      var sql_command = `      
                                merge into  "DATAHUB_TARGET"."EAM_R5FIRSTACT" as target using (
                                  
                      
                  
                    SELECT * FROM (SELECT
                    
                    
                        strm.ACT_EVENT,
                        strm.ACT_ACT,
                        strm.ACT_START,
                        strm.ACT_TRADE,
                        strm.ACT_PERSONS,
                        strm.ACT_DURATION,
                        strm.ACT_EST,
                        strm.ACT_REM,
                        strm.ACT_TASK,
                        strm.ACT_MATLIST,
                        strm.ACT_MULTIPLETRADES,
                        strm.ACT_RPC,
                        strm.ACT_WAP,
                        strm.ACT_TPF,
                        strm.ACT_MANUFACT,
                        strm.ACT_SYSLEVEL,
                        strm.ACT_ASMLEVEL,
                        strm.ACT_COMPLEVEL,
                        strm.ACT_COMPLOCATION,
                        strm.ACT_LASTSAVED,
                        strm.ETL_LASTSAVED,
                        strm.ETL_DELETED,
                        strm.etl_load_datetime,
                      strm.etl_load_metadatafilename,
                      ROW_NUMBER() OVER (PARTITION BY 
                                        strm.ACT_EVENT  ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                      
                     FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FIRSTACT as  strm
                     )   
                WHERE ROWNUMBER=1  
               
                     ) as src on
                                ((target.ACT_EVENT=src.ACT_EVENT) OR (target.ACT_EVENT IS NULL AND src.ACT_EVENT IS NULL))
                    when matched and src.ETL_DELETED=TRUE THEN DELETE
                    when matched then update set
                                target.ACT_EVENT=src.ACT_EVENT,
                                target.ACT_ACT=src.ACT_ACT,
                                target.ACT_START=src.ACT_START,
                                target.ACT_TRADE=src.ACT_TRADE,
                                target.ACT_PERSONS=src.ACT_PERSONS,
                                target.ACT_DURATION=src.ACT_DURATION,
                                target.ACT_EST=src.ACT_EST,
                                target.ACT_REM=src.ACT_REM,
                                target.ACT_TASK=src.ACT_TASK,
                                target.ACT_MATLIST=src.ACT_MATLIST,
                                target.ACT_MULTIPLETRADES=src.ACT_MULTIPLETRADES,
                                target.ACT_RPC=src.ACT_RPC,
                                target.ACT_WAP=src.ACT_WAP,
                                target.ACT_TPF=src.ACT_TPF,
                                target.ACT_MANUFACT=src.ACT_MANUFACT,
                                target.ACT_SYSLEVEL=src.ACT_SYSLEVEL,
                                target.ACT_ASMLEVEL=src.ACT_ASMLEVEL,
                                target.ACT_COMPLEVEL=src.ACT_COMPLEVEL,
                                target.ACT_COMPLOCATION=src.ACT_COMPLOCATION,
                                target.ACT_LASTSAVED=src.ACT_LASTSAVED,
                  target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                        target.etl_load_datetime=CURRENT_TIMESTAMP,
                      target.etl_load_metadatafilename=src.etl_load_metadatafilename 
  when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                                ACT_EVENT, 
                                ACT_ACT, 
                                ACT_START, 
                                ACT_TRADE, 
                                ACT_PERSONS, 
                                ACT_DURATION, 
                                ACT_EST, 
                                ACT_REM, 
                                ACT_TASK, 
                                ACT_MATLIST, 
                                ACT_MULTIPLETRADES, 
                                ACT_RPC, 
                                ACT_WAP, 
                                ACT_TPF, 
                                ACT_MANUFACT, 
                                ACT_SYSLEVEL, 
                                ACT_ASMLEVEL, 
                                ACT_COMPLEVEL, 
                                ACT_COMPLOCATION, 
                                ACT_LASTSAVED,
                      ETL_LASTSAVED,
                       
                        etl_load_datetime,
                      etl_load_metadatafilename) values (
                                 src.ACT_EVENT,
                                 src.ACT_ACT,
                                 src.ACT_START,
                                 src.ACT_TRADE,
                                 src.ACT_PERSONS,
                                 src.ACT_DURATION,
                                 src.ACT_EST,
                                 src.ACT_REM,
                                 src.ACT_TASK,
                                 src.ACT_MATLIST,
                                 src.ACT_MULTIPLETRADES,
                                 src.ACT_RPC,
                                 src.ACT_WAP,
                                 src.ACT_TPF,
                                 src.ACT_MANUFACT,
                                 src.ACT_SYSLEVEL,
                                 src.ACT_ASMLEVEL,
                                 src.ACT_COMPLEVEL,
                                 src.ACT_COMPLOCATION,
                                 src.ACT_LASTSAVED,
                       src.ETL_LASTSAVED,
                        CURRENT_TIMESTAMP,
                      src.etl_load_metadatafilename);`;    snowflake.execute( {sqlText: 
`
  merge into  "DATAHUB_TARGET_HISTORY"."EAM_DELETED_R5FIRSTACT" as target using (
    
     
     SELECT * FROM (SELECT
    
    
                        strm.ACT_EVENT,
                        strm.ACT_ACT,
                        strm.ACT_START,
                        strm.ACT_TRADE,
                        strm.ACT_PERSONS,
                        strm.ACT_DURATION,
                        strm.ACT_EST,
                        strm.ACT_REM,
                        strm.ACT_TASK,
                        strm.ACT_MATLIST,
                        strm.ACT_MULTIPLETRADES,
                        strm.ACT_RPC,
                        strm.ACT_WAP,
                        strm.ACT_TPF,
                        strm.ACT_MANUFACT,
                        strm.ACT_SYSLEVEL,
                        strm.ACT_ASMLEVEL,
                        strm.ACT_COMPLEVEL,
                        strm.ACT_COMPLOCATION,
                        strm.ACT_LASTSAVED,
                        strm.ETL_LASTSAVED,
                        strm.ETL_DELETED,
                        strm.etl_load_datetime,
                      strm.etl_load_metadatafilename,
                      ROW_NUMBER() OVER (PARTITION BY 
                                        strm.ACT_EVENT  ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                      
                     FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FIRSTACT as  strm
                     WHERE strm.ETL_DELETED=TRUE 
                     )   
                WHERE ROWNUMBER=1  
         
                     ) as src on
                                ((target.ACT_EVENT=src.ACT_EVENT) OR (target.ACT_EVENT IS NULL AND src.ACT_EVENT IS NULL))
                    when matched then update set
                                target.ACT_EVENT=src.ACT_EVENT,
                                target.ACT_ACT=src.ACT_ACT,
                                target.ACT_START=src.ACT_START,
                                target.ACT_TRADE=src.ACT_TRADE,
                                target.ACT_PERSONS=src.ACT_PERSONS,
                                target.ACT_DURATION=src.ACT_DURATION,
                                target.ACT_EST=src.ACT_EST,
                                target.ACT_REM=src.ACT_REM,
                                target.ACT_TASK=src.ACT_TASK,
                                target.ACT_MATLIST=src.ACT_MATLIST,
                                target.ACT_MULTIPLETRADES=src.ACT_MULTIPLETRADES,
                                target.ACT_RPC=src.ACT_RPC,
                                target.ACT_WAP=src.ACT_WAP,
                                target.ACT_TPF=src.ACT_TPF,
                                target.ACT_MANUFACT=src.ACT_MANUFACT,
                                target.ACT_SYSLEVEL=src.ACT_SYSLEVEL,
                                target.ACT_ASMLEVEL=src.ACT_ASMLEVEL,
                                target.ACT_COMPLEVEL=src.ACT_COMPLEVEL,
                                target.ACT_COMPLOCATION=src.ACT_COMPLOCATION,
                                target.ACT_LASTSAVED=src.ACT_LASTSAVED,
                  target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                        target.etl_load_datetime=CURRENT_TIMESTAMP,
                      target.etl_load_metadatafilename=src.etl_load_metadatafilename 
  when not matched then insert ( 
                                ACT_EVENT, 
                                ACT_ACT, 
                                ACT_START, 
                                ACT_TRADE, 
                                ACT_PERSONS, 
                                ACT_DURATION, 
                                ACT_EST, 
                                ACT_REM, 
                                ACT_TASK, 
                                ACT_MATLIST, 
                                ACT_MULTIPLETRADES, 
                                ACT_RPC, 
                                ACT_WAP, 
                                ACT_TPF, 
                                ACT_MANUFACT, 
                                ACT_SYSLEVEL, 
                                ACT_ASMLEVEL, 
                                ACT_COMPLEVEL, 
                                ACT_COMPLOCATION, 
                                ACT_LASTSAVED,
                      ETL_LASTSAVED,
                       
                        etl_load_datetime,
                      etl_load_metadatafilename,
etl_is_deleted,ETL_DELETED) values (
                                 src.ACT_EVENT,
                                 src.ACT_ACT,
                                 src.ACT_START,
                                 src.ACT_TRADE,
                                 src.ACT_PERSONS,
                                 src.ACT_DURATION,
                                 src.ACT_EST,
                                 src.ACT_REM,
                                 src.ACT_TASK,
                                 src.ACT_MATLIST,
                                 src.ACT_MULTIPLETRADES,
                                 src.ACT_RPC,
                                 src.ACT_WAP,
                                 src.ACT_TPF,
                                 src.ACT_MANUFACT,
                                 src.ACT_SYSLEVEL,
                                 src.ACT_ASMLEVEL,
                                 src.ACT_COMPLEVEL,
                                 src.ACT_COMPLOCATION,
                                 src.ACT_LASTSAVED,
                       src.ETL_LASTSAVED,
                        CURRENT_TIMESTAMP,
                      src.etl_load_metadatafilename
                      ,case when ETL_DELETED=TRUE then TRUE else FALSE end 
                      ,ETL_DELETED);
`
   } );
       
            
  //  Get number of rows inserted
        
        try
            {
            
            var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
            var rs = sqlStmt.execute();
            var number_of_rows_inserted = sqlStmt.getNumRowsInserted();
         //   var number_of_rows_updated =  sqlStmt.getNumRowsUpdated(); 
            var number_of_rows_updated =  sqlStmt.getNumRowsUpdated();
            
            }
         catch (err)
         {

            
            if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug('" + process_name + "','Failed to get number of rows inserted, err = " + err + "');";
            snowflake.execute({sqlText: debug_statement}); 
            }
			
						
			//  Create a log entry to say INSERT STATEMENT failed
        
	        log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Failed','Failed to insert the rows. Quitting.', " + process_execution_id.toString() + ",0,0);";
	        snowflake.execute({sqlText: log_sql_command});
	        return "Failed"; 
         }

         
        snowflake.execute ({sqlText: "commit"});

        log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Success','Completed process execution.', " + process_execution_id.toString() + "," + number_of_rows_inserted.toString()+ "," +  number_of_rows_updated.toString() + ");";
        snowflake.execute({sqlText: log_sql_command});

        result = "Success"; 

        //  log debug message
        if( debug === "True" ) 
            {
            debug_statement = "call datahub_logging.sp_etl_log_ingestion_process_debug('" + process_name + "','Commit Transaction');";
            snowflake.execute({sqlText: debug_statement}); 
            }

        }       

        catch (err)  {
            snowflake.execute( {sqlText: "rollback;"} );
            return "Failed: " + err;   // Return a success/error indicator.
            }


  return result;
  $$;
