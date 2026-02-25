
                    
                create or replace procedure DATAHUB_INTEGRATION.EAM_TARGET_MERGE_R5JOBPARAM()
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
                                merge into  "DATAHUB_TARGET"."EAM_R5JOBPARAM" as target using (
                                  
                      
                  
                    SELECT * FROM (SELECT
                    
                    
                        strm.JPR_NAME,
                        strm.JPR_LINE,
                        strm.JPR_PARAMETER,
                        strm.JPR_RENTITY,
                        strm.JPR_RTYPE,
                        strm.JPR_DATATYPE,
                        strm.JPR_LENGTH,
                        strm.JPR_FORMAT,
                        strm.JPR_DEFAULT,
                        strm.JPR_FIXED,
                        strm.JPR_MANDATORY,
                        strm.JPR_UPPER,
                        strm.JPR_OPTIONS,
                        strm.JPR_REMEMBER,
                        strm.JPR_TEST,
                        strm.JPR_QUERY,
                        strm.JPR_LOVFUNCTION,
                        strm.JPR_PROPERTY,
                        strm.JPR_EWSLOVDEF,
                        strm.JPR_UPDATECOUNT,
                        strm.JPR_LASTSAVED,
                        strm.ETL_LASTSAVED,
                        strm.ETL_DELETED,
                        strm.etl_load_datetime,
                      strm.etl_load_metadatafilename,
                      ROW_NUMBER() OVER (PARTITION BY   ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                      
                     FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5JOBPARAM as  strm
                     )   
                WHERE ROWNUMBER=1  
               
                     ) as src on
                    when matched and src.ETL_DELETED=TRUE THEN DELETE
                    when matched then update set
                                target.JPR_NAME=src.JPR_NAME,
                                target.JPR_LINE=src.JPR_LINE,
                                target.JPR_PARAMETER=src.JPR_PARAMETER,
                                target.JPR_RENTITY=src.JPR_RENTITY,
                                target.JPR_RTYPE=src.JPR_RTYPE,
                                target.JPR_DATATYPE=src.JPR_DATATYPE,
                                target.JPR_LENGTH=src.JPR_LENGTH,
                                target.JPR_FORMAT=src.JPR_FORMAT,
                                target.JPR_DEFAULT=src.JPR_DEFAULT,
                                target.JPR_FIXED=src.JPR_FIXED,
                                target.JPR_MANDATORY=src.JPR_MANDATORY,
                                target.JPR_UPPER=src.JPR_UPPER,
                                target.JPR_OPTIONS=src.JPR_OPTIONS,
                                target.JPR_REMEMBER=src.JPR_REMEMBER,
                                target.JPR_TEST=src.JPR_TEST,
                                target.JPR_QUERY=src.JPR_QUERY,
                                target.JPR_LOVFUNCTION=src.JPR_LOVFUNCTION,
                                target.JPR_PROPERTY=src.JPR_PROPERTY,
                                target.JPR_EWSLOVDEF=src.JPR_EWSLOVDEF,
                                target.JPR_UPDATECOUNT=src.JPR_UPDATECOUNT,
                                target.JPR_LASTSAVED=src.JPR_LASTSAVED,
                  target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                        target.etl_load_datetime=CURRENT_TIMESTAMP,
                      target.etl_load_metadatafilename=src.etl_load_metadatafilename 
  when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                                JPR_NAME, 
                                JPR_LINE, 
                                JPR_PARAMETER, 
                                JPR_RENTITY, 
                                JPR_RTYPE, 
                                JPR_DATATYPE, 
                                JPR_LENGTH, 
                                JPR_FORMAT, 
                                JPR_DEFAULT, 
                                JPR_FIXED, 
                                JPR_MANDATORY, 
                                JPR_UPPER, 
                                JPR_OPTIONS, 
                                JPR_REMEMBER, 
                                JPR_TEST, 
                                JPR_QUERY, 
                                JPR_LOVFUNCTION, 
                                JPR_PROPERTY, 
                                JPR_EWSLOVDEF, 
                                JPR_UPDATECOUNT, 
                                JPR_LASTSAVED,
                      ETL_LASTSAVED,
                       
                        etl_load_datetime,
                      etl_load_metadatafilename) values (
                                 src.JPR_NAME,
                                 src.JPR_LINE,
                                 src.JPR_PARAMETER,
                                 src.JPR_RENTITY,
                                 src.JPR_RTYPE,
                                 src.JPR_DATATYPE,
                                 src.JPR_LENGTH,
                                 src.JPR_FORMAT,
                                 src.JPR_DEFAULT,
                                 src.JPR_FIXED,
                                 src.JPR_MANDATORY,
                                 src.JPR_UPPER,
                                 src.JPR_OPTIONS,
                                 src.JPR_REMEMBER,
                                 src.JPR_TEST,
                                 src.JPR_QUERY,
                                 src.JPR_LOVFUNCTION,
                                 src.JPR_PROPERTY,
                                 src.JPR_EWSLOVDEF,
                                 src.JPR_UPDATECOUNT,
                                 src.JPR_LASTSAVED,
                       src.ETL_LASTSAVED,
                        CURRENT_TIMESTAMP,
                      src.etl_load_metadatafilename);`;    snowflake.execute( {sqlText: 
`
  merge into  "DATAHUB_TARGET_HISTORY"."EAM_DELETED_R5JOBPARAM" as target using (
    
     
     SELECT * FROM (SELECT
    
    
                        strm.JPR_NAME,
                        strm.JPR_LINE,
                        strm.JPR_PARAMETER,
                        strm.JPR_RENTITY,
                        strm.JPR_RTYPE,
                        strm.JPR_DATATYPE,
                        strm.JPR_LENGTH,
                        strm.JPR_FORMAT,
                        strm.JPR_DEFAULT,
                        strm.JPR_FIXED,
                        strm.JPR_MANDATORY,
                        strm.JPR_UPPER,
                        strm.JPR_OPTIONS,
                        strm.JPR_REMEMBER,
                        strm.JPR_TEST,
                        strm.JPR_QUERY,
                        strm.JPR_LOVFUNCTION,
                        strm.JPR_PROPERTY,
                        strm.JPR_EWSLOVDEF,
                        strm.JPR_UPDATECOUNT,
                        strm.JPR_LASTSAVED,
                        strm.ETL_LASTSAVED,
                        strm.ETL_DELETED,
                        strm.etl_load_datetime,
                      strm.etl_load_metadatafilename,
                      ROW_NUMBER() OVER (PARTITION BY   ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                      
                     FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5JOBPARAM as  strm
                     WHERE strm.ETL_DELETED=TRUE 
                     )   
                WHERE ROWNUMBER=1  
         
                     ) as src on
                    when matched then update set
                                target.JPR_NAME=src.JPR_NAME,
                                target.JPR_LINE=src.JPR_LINE,
                                target.JPR_PARAMETER=src.JPR_PARAMETER,
                                target.JPR_RENTITY=src.JPR_RENTITY,
                                target.JPR_RTYPE=src.JPR_RTYPE,
                                target.JPR_DATATYPE=src.JPR_DATATYPE,
                                target.JPR_LENGTH=src.JPR_LENGTH,
                                target.JPR_FORMAT=src.JPR_FORMAT,
                                target.JPR_DEFAULT=src.JPR_DEFAULT,
                                target.JPR_FIXED=src.JPR_FIXED,
                                target.JPR_MANDATORY=src.JPR_MANDATORY,
                                target.JPR_UPPER=src.JPR_UPPER,
                                target.JPR_OPTIONS=src.JPR_OPTIONS,
                                target.JPR_REMEMBER=src.JPR_REMEMBER,
                                target.JPR_TEST=src.JPR_TEST,
                                target.JPR_QUERY=src.JPR_QUERY,
                                target.JPR_LOVFUNCTION=src.JPR_LOVFUNCTION,
                                target.JPR_PROPERTY=src.JPR_PROPERTY,
                                target.JPR_EWSLOVDEF=src.JPR_EWSLOVDEF,
                                target.JPR_UPDATECOUNT=src.JPR_UPDATECOUNT,
                                target.JPR_LASTSAVED=src.JPR_LASTSAVED,
                  target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                        target.etl_load_datetime=CURRENT_TIMESTAMP,
                      target.etl_load_metadatafilename=src.etl_load_metadatafilename 
  when not matched then insert ( 
                                JPR_NAME, 
                                JPR_LINE, 
                                JPR_PARAMETER, 
                                JPR_RENTITY, 
                                JPR_RTYPE, 
                                JPR_DATATYPE, 
                                JPR_LENGTH, 
                                JPR_FORMAT, 
                                JPR_DEFAULT, 
                                JPR_FIXED, 
                                JPR_MANDATORY, 
                                JPR_UPPER, 
                                JPR_OPTIONS, 
                                JPR_REMEMBER, 
                                JPR_TEST, 
                                JPR_QUERY, 
                                JPR_LOVFUNCTION, 
                                JPR_PROPERTY, 
                                JPR_EWSLOVDEF, 
                                JPR_UPDATECOUNT, 
                                JPR_LASTSAVED,
                      ETL_LASTSAVED,
                       
                        etl_load_datetime,
                      etl_load_metadatafilename,
etl_is_deleted,ETL_DELETED) values (
                                 src.JPR_NAME,
                                 src.JPR_LINE,
                                 src.JPR_PARAMETER,
                                 src.JPR_RENTITY,
                                 src.JPR_RTYPE,
                                 src.JPR_DATATYPE,
                                 src.JPR_LENGTH,
                                 src.JPR_FORMAT,
                                 src.JPR_DEFAULT,
                                 src.JPR_FIXED,
                                 src.JPR_MANDATORY,
                                 src.JPR_UPPER,
                                 src.JPR_OPTIONS,
                                 src.JPR_REMEMBER,
                                 src.JPR_TEST,
                                 src.JPR_QUERY,
                                 src.JPR_LOVFUNCTION,
                                 src.JPR_PROPERTY,
                                 src.JPR_EWSLOVDEF,
                                 src.JPR_UPDATECOUNT,
                                 src.JPR_LASTSAVED,
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
