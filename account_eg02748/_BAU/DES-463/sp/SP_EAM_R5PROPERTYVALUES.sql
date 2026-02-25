
                    
                create or replace procedure DATAHUB_INTEGRATION.EAM_TARGET_MERGE_R5PROPERTYVALUES()
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
                                merge into  "DATAHUB_TARGET"."EAM_R5PROPERTYVALUES" as target using (
                                  
                      
                  
                    SELECT * FROM (SELECT
                    
                    
                        strm.PRV_PROPERTY,
                        strm.PRV_RENTITY,
                        strm.PRV_CLASS,
                        strm.PRV_CODE,
                        strm.PRV_SEQNO,
                        strm.PRV_VALUE,
                        strm.PRV_NVALUE,
                        strm.PRV_DVALUE,
                        strm.PRV_CLASS_ORG,
                        strm.PRV_UPDATECOUNT,
                        strm.PRV_CREATED,
                        strm.PRV_UPDATED,
                        strm.PRV_NOTUSED,
                        strm.PRV_LASTSAVED,
                        strm.ETL_LASTSAVED,
                        strm.ETL_DELETED,
                        strm.etl_load_datetime,
                      strm.etl_load_metadatafilename,
                      ROW_NUMBER() OVER (PARTITION BY   ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                      
                     FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PROPERTYVALUES as  strm
                     )   
                WHERE ROWNUMBER=1  
               
                     ) as src on
                    when matched and src.ETL_DELETED=TRUE THEN DELETE
                    when matched then update set
                                target.PRV_PROPERTY=src.PRV_PROPERTY,
                                target.PRV_RENTITY=src.PRV_RENTITY,
                                target.PRV_CLASS=src.PRV_CLASS,
                                target.PRV_CODE=src.PRV_CODE,
                                target.PRV_SEQNO=src.PRV_SEQNO,
                                target.PRV_VALUE=src.PRV_VALUE,
                                target.PRV_NVALUE=src.PRV_NVALUE,
                                target.PRV_DVALUE=src.PRV_DVALUE,
                                target.PRV_CLASS_ORG=src.PRV_CLASS_ORG,
                                target.PRV_UPDATECOUNT=src.PRV_UPDATECOUNT,
                                target.PRV_CREATED=src.PRV_CREATED,
                                target.PRV_UPDATED=src.PRV_UPDATED,
                                target.PRV_NOTUSED=src.PRV_NOTUSED,
                                target.PRV_LASTSAVED=src.PRV_LASTSAVED,
                  target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                        target.etl_load_datetime=CURRENT_TIMESTAMP,
                      target.etl_load_metadatafilename=src.etl_load_metadatafilename 
  when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                                PRV_PROPERTY, 
                                PRV_RENTITY, 
                                PRV_CLASS, 
                                PRV_CODE, 
                                PRV_SEQNO, 
                                PRV_VALUE, 
                                PRV_NVALUE, 
                                PRV_DVALUE, 
                                PRV_CLASS_ORG, 
                                PRV_UPDATECOUNT, 
                                PRV_CREATED, 
                                PRV_UPDATED, 
                                PRV_NOTUSED, 
                                PRV_LASTSAVED,
                      ETL_LASTSAVED,
                       
                        etl_load_datetime,
                      etl_load_metadatafilename) values (
                                 src.PRV_PROPERTY,
                                 src.PRV_RENTITY,
                                 src.PRV_CLASS,
                                 src.PRV_CODE,
                                 src.PRV_SEQNO,
                                 src.PRV_VALUE,
                                 src.PRV_NVALUE,
                                 src.PRV_DVALUE,
                                 src.PRV_CLASS_ORG,
                                 src.PRV_UPDATECOUNT,
                                 src.PRV_CREATED,
                                 src.PRV_UPDATED,
                                 src.PRV_NOTUSED,
                                 src.PRV_LASTSAVED,
                       src.ETL_LASTSAVED,
                        CURRENT_TIMESTAMP,
                      src.etl_load_metadatafilename);`;    snowflake.execute( {sqlText: 
`
  merge into  "DATAHUB_TARGET_HISTORY"."EAM_DELETED_R5PROPERTYVALUES" as target using (
    
     
     SELECT * FROM (SELECT
    
    
                        strm.PRV_PROPERTY,
                        strm.PRV_RENTITY,
                        strm.PRV_CLASS,
                        strm.PRV_CODE,
                        strm.PRV_SEQNO,
                        strm.PRV_VALUE,
                        strm.PRV_NVALUE,
                        strm.PRV_DVALUE,
                        strm.PRV_CLASS_ORG,
                        strm.PRV_UPDATECOUNT,
                        strm.PRV_CREATED,
                        strm.PRV_UPDATED,
                        strm.PRV_NOTUSED,
                        strm.PRV_LASTSAVED,
                        strm.ETL_LASTSAVED,
                        strm.ETL_DELETED,
                        strm.etl_load_datetime,
                      strm.etl_load_metadatafilename,
                      ROW_NUMBER() OVER (PARTITION BY   ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                      
                     FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PROPERTYVALUES as  strm
                     WHERE strm.ETL_DELETED=TRUE 
                     )   
                WHERE ROWNUMBER=1  
         
                     ) as src on
                    when matched then update set
                                target.PRV_PROPERTY=src.PRV_PROPERTY,
                                target.PRV_RENTITY=src.PRV_RENTITY,
                                target.PRV_CLASS=src.PRV_CLASS,
                                target.PRV_CODE=src.PRV_CODE,
                                target.PRV_SEQNO=src.PRV_SEQNO,
                                target.PRV_VALUE=src.PRV_VALUE,
                                target.PRV_NVALUE=src.PRV_NVALUE,
                                target.PRV_DVALUE=src.PRV_DVALUE,
                                target.PRV_CLASS_ORG=src.PRV_CLASS_ORG,
                                target.PRV_UPDATECOUNT=src.PRV_UPDATECOUNT,
                                target.PRV_CREATED=src.PRV_CREATED,
                                target.PRV_UPDATED=src.PRV_UPDATED,
                                target.PRV_NOTUSED=src.PRV_NOTUSED,
                                target.PRV_LASTSAVED=src.PRV_LASTSAVED,
                  target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                        target.etl_load_datetime=CURRENT_TIMESTAMP,
                      target.etl_load_metadatafilename=src.etl_load_metadatafilename 
  when not matched then insert ( 
                                PRV_PROPERTY, 
                                PRV_RENTITY, 
                                PRV_CLASS, 
                                PRV_CODE, 
                                PRV_SEQNO, 
                                PRV_VALUE, 
                                PRV_NVALUE, 
                                PRV_DVALUE, 
                                PRV_CLASS_ORG, 
                                PRV_UPDATECOUNT, 
                                PRV_CREATED, 
                                PRV_UPDATED, 
                                PRV_NOTUSED, 
                                PRV_LASTSAVED,
                      ETL_LASTSAVED,
                       
                        etl_load_datetime,
                      etl_load_metadatafilename,
etl_is_deleted,ETL_DELETED) values (
                                 src.PRV_PROPERTY,
                                 src.PRV_RENTITY,
                                 src.PRV_CLASS,
                                 src.PRV_CODE,
                                 src.PRV_SEQNO,
                                 src.PRV_VALUE,
                                 src.PRV_NVALUE,
                                 src.PRV_DVALUE,
                                 src.PRV_CLASS_ORG,
                                 src.PRV_UPDATECOUNT,
                                 src.PRV_CREATED,
                                 src.PRV_UPDATED,
                                 src.PRV_NOTUSED,
                                 src.PRV_LASTSAVED,
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
