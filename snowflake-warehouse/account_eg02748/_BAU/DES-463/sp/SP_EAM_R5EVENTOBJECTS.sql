
                    
                create or replace procedure DATAHUB_INTEGRATION.EAM_TARGET_MERGE_R5EVENTOBJECTS()
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
                                merge into  "DATAHUB_TARGET"."EAM_R5EVENTOBJECTS" as target using (
                                  
                      
                  
                    SELECT * FROM (SELECT
                    
                    
                        strm.EOB_EVENT,
                        strm.EOB_OBTYPE,
                        strm.EOB_OBRTYPE,
                        strm.EOB_OBJECT,
                        strm.EOB_LEVEL,
                        strm.EOB_ROLLUP,
                        strm.EOB_DOWNTIME,
                        strm.EOB_OBJECT_ORG,
                        strm.EOB_UPDATECOUNT,
                        strm.EOB_FROMPOINT,
                        strm.EOB_TOPOINT,
                        strm.EOB_WEIGHTPERCENTAGE,
                        strm.EOB_LASTSAVED,
                        strm.ETL_LASTSAVED,
                        strm.ETL_DELETED,
                        strm.etl_load_datetime,
                      strm.etl_load_metadatafilename,
                      ROW_NUMBER() OVER (PARTITION BY 
                                        strm.EOB_EVENT,
                                                
                                        strm.EOB_OBJECT,
                                                
                                        strm.EOB_OBJECT_ORG  ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                      
                     FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5EVENTOBJECTS as  strm
                     )   
                WHERE ROWNUMBER=1  
               
                     ) as src on
                                ((target.EOB_EVENT=src.EOB_EVENT) OR (target.EOB_EVENT IS NULL AND src.EOB_EVENT IS NULL)) and
                                                
                                ((target.EOB_OBJECT=src.EOB_OBJECT) OR (target.EOB_OBJECT IS NULL AND src.EOB_OBJECT IS NULL)) and
                                                
                                ((target.EOB_OBJECT_ORG=src.EOB_OBJECT_ORG) OR (target.EOB_OBJECT_ORG IS NULL AND src.EOB_OBJECT_ORG IS NULL))
                    when matched and src.ETL_DELETED=TRUE THEN DELETE
                    when matched then update set
                                target.EOB_EVENT=src.EOB_EVENT,
                                target.EOB_OBTYPE=src.EOB_OBTYPE,
                                target.EOB_OBRTYPE=src.EOB_OBRTYPE,
                                target.EOB_OBJECT=src.EOB_OBJECT,
                                target.EOB_LEVEL=src.EOB_LEVEL,
                                target.EOB_ROLLUP=src.EOB_ROLLUP,
                                target.EOB_DOWNTIME=src.EOB_DOWNTIME,
                                target.EOB_OBJECT_ORG=src.EOB_OBJECT_ORG,
                                target.EOB_UPDATECOUNT=src.EOB_UPDATECOUNT,
                                target.EOB_FROMPOINT=src.EOB_FROMPOINT,
                                target.EOB_TOPOINT=src.EOB_TOPOINT,
                                target.EOB_WEIGHTPERCENTAGE=src.EOB_WEIGHTPERCENTAGE,
                                target.EOB_LASTSAVED=src.EOB_LASTSAVED,
                  target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                        target.etl_load_datetime=CURRENT_TIMESTAMP,
                      target.etl_load_metadatafilename=src.etl_load_metadatafilename 
  when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                                EOB_EVENT, 
                                EOB_OBTYPE, 
                                EOB_OBRTYPE, 
                                EOB_OBJECT, 
                                EOB_LEVEL, 
                                EOB_ROLLUP, 
                                EOB_DOWNTIME, 
                                EOB_OBJECT_ORG, 
                                EOB_UPDATECOUNT, 
                                EOB_FROMPOINT, 
                                EOB_TOPOINT, 
                                EOB_WEIGHTPERCENTAGE, 
                                EOB_LASTSAVED,
                      ETL_LASTSAVED,
                       
                        etl_load_datetime,
                      etl_load_metadatafilename) values (
                                 src.EOB_EVENT,
                                 src.EOB_OBTYPE,
                                 src.EOB_OBRTYPE,
                                 src.EOB_OBJECT,
                                 src.EOB_LEVEL,
                                 src.EOB_ROLLUP,
                                 src.EOB_DOWNTIME,
                                 src.EOB_OBJECT_ORG,
                                 src.EOB_UPDATECOUNT,
                                 src.EOB_FROMPOINT,
                                 src.EOB_TOPOINT,
                                 src.EOB_WEIGHTPERCENTAGE,
                                 src.EOB_LASTSAVED,
                       src.ETL_LASTSAVED,
                        CURRENT_TIMESTAMP,
                      src.etl_load_metadatafilename);`;    snowflake.execute( {sqlText: 
`
  merge into  "DATAHUB_TARGET_HISTORY"."EAM_DELETED_R5EVENTOBJECTS" as target using (
    
     
     SELECT * FROM (SELECT
    
    
                        strm.EOB_EVENT,
                        strm.EOB_OBTYPE,
                        strm.EOB_OBRTYPE,
                        strm.EOB_OBJECT,
                        strm.EOB_LEVEL,
                        strm.EOB_ROLLUP,
                        strm.EOB_DOWNTIME,
                        strm.EOB_OBJECT_ORG,
                        strm.EOB_UPDATECOUNT,
                        strm.EOB_FROMPOINT,
                        strm.EOB_TOPOINT,
                        strm.EOB_WEIGHTPERCENTAGE,
                        strm.EOB_LASTSAVED,
                        strm.ETL_LASTSAVED,
                        strm.ETL_DELETED,
                        strm.etl_load_datetime,
                      strm.etl_load_metadatafilename,
                      ROW_NUMBER() OVER (PARTITION BY 
                                        strm.EOB_EVENT,
                                                
                                        strm.EOB_OBJECT,
                                                
                                        strm.EOB_OBJECT_ORG  ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                      
                     FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5EVENTOBJECTS as  strm
                     WHERE strm.ETL_DELETED=TRUE 
                     )   
                WHERE ROWNUMBER=1  
         
                     ) as src on
                                ((target.EOB_EVENT=src.EOB_EVENT) OR (target.EOB_EVENT IS NULL AND src.EOB_EVENT IS NULL)) and
                                                
                                ((target.EOB_OBJECT=src.EOB_OBJECT) OR (target.EOB_OBJECT IS NULL AND src.EOB_OBJECT IS NULL)) and
                                                
                                ((target.EOB_OBJECT_ORG=src.EOB_OBJECT_ORG) OR (target.EOB_OBJECT_ORG IS NULL AND src.EOB_OBJECT_ORG IS NULL))
                    when matched then update set
                                target.EOB_EVENT=src.EOB_EVENT,
                                target.EOB_OBTYPE=src.EOB_OBTYPE,
                                target.EOB_OBRTYPE=src.EOB_OBRTYPE,
                                target.EOB_OBJECT=src.EOB_OBJECT,
                                target.EOB_LEVEL=src.EOB_LEVEL,
                                target.EOB_ROLLUP=src.EOB_ROLLUP,
                                target.EOB_DOWNTIME=src.EOB_DOWNTIME,
                                target.EOB_OBJECT_ORG=src.EOB_OBJECT_ORG,
                                target.EOB_UPDATECOUNT=src.EOB_UPDATECOUNT,
                                target.EOB_FROMPOINT=src.EOB_FROMPOINT,
                                target.EOB_TOPOINT=src.EOB_TOPOINT,
                                target.EOB_WEIGHTPERCENTAGE=src.EOB_WEIGHTPERCENTAGE,
                                target.EOB_LASTSAVED=src.EOB_LASTSAVED,
                  target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                        target.etl_load_datetime=CURRENT_TIMESTAMP,
                      target.etl_load_metadatafilename=src.etl_load_metadatafilename 
  when not matched then insert ( 
                                EOB_EVENT, 
                                EOB_OBTYPE, 
                                EOB_OBRTYPE, 
                                EOB_OBJECT, 
                                EOB_LEVEL, 
                                EOB_ROLLUP, 
                                EOB_DOWNTIME, 
                                EOB_OBJECT_ORG, 
                                EOB_UPDATECOUNT, 
                                EOB_FROMPOINT, 
                                EOB_TOPOINT, 
                                EOB_WEIGHTPERCENTAGE, 
                                EOB_LASTSAVED,
                      ETL_LASTSAVED,
                       
                        etl_load_datetime,
                      etl_load_metadatafilename,
etl_is_deleted,ETL_DELETED) values (
                                 src.EOB_EVENT,
                                 src.EOB_OBTYPE,
                                 src.EOB_OBRTYPE,
                                 src.EOB_OBJECT,
                                 src.EOB_LEVEL,
                                 src.EOB_ROLLUP,
                                 src.EOB_DOWNTIME,
                                 src.EOB_OBJECT_ORG,
                                 src.EOB_UPDATECOUNT,
                                 src.EOB_FROMPOINT,
                                 src.EOB_TOPOINT,
                                 src.EOB_WEIGHTPERCENTAGE,
                                 src.EOB_LASTSAVED,
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
