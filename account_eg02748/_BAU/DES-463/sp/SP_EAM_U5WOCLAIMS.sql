
                    
                create or replace procedure DATAHUB_INTEGRATION.EAM_TARGET_MERGE_U5WOCLAIMS()
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
                                merge into  "DATAHUB_TARGET"."EAM_U5WOCLAIMS" as target using (
                                  
                      
                  
                    SELECT * FROM (SELECT
                    
                    
                        strm.WCO_EVENT,
                        strm.WCO_PK,
                        strm.WCO_ORG,
                        strm.WCO_SCHEDULE_ITEM,
                        strm.WCO_TRANSID,
                        strm.WCO_CONTRACTOR_QTY,
                        strm.WCO_CONTRACTOR_RATE,
                        strm.WCO_CHARGEDATE,
                        strm.WCO_COMMENTS,
                        strm.WCO_SCHEDITEM_DESC,
                        strm.WCO_SCHEDITEM_RATE,
                        strm.WCO_LINETOTAL,
                        strm.WCO_WOSCHEDITEM,
                        strm.WCO_WOTYPE,
                        strm.WCO_WOPARENT,
                        strm.WCO_CONTRACT_CODE,
                        strm.WCO_CONTRACTOR,
                        strm.WCO_ACTIVITY,
                        strm.WCO_ACTIVITY_DESC,
                        strm.CREATEDBY,
                        strm.CREATED,
                        strm.UPDATEDBY,
                        strm.UPDATED,
                        strm.LASTSAVED,
                        strm.UPDATECOUNT,
                        strm.WCO_LINE_STATUS,
                        strm.ETL_LASTSAVED,
                        strm.ETL_DELETED,
                        strm.etl_load_datetime,
                      strm.etl_load_metadatafilename,
                      ROW_NUMBER() OVER (PARTITION BY 
                                        strm.WCO_EVENT,
                                                
                                        strm.WCO_PK,
                                                
                                        strm.WCO_ORG  ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                      
                     FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5WOCLAIMS as  strm
                     )   
                WHERE ROWNUMBER=1  
               
                     ) as src on
                                ((target.WCO_EVENT=src.WCO_EVENT) OR (target.WCO_EVENT IS NULL AND src.WCO_EVENT IS NULL)) and
                                                
                                ((target.WCO_PK=src.WCO_PK) OR (target.WCO_PK IS NULL AND src.WCO_PK IS NULL)) and
                                                
                                ((target.WCO_ORG=src.WCO_ORG) OR (target.WCO_ORG IS NULL AND src.WCO_ORG IS NULL))
                    when matched and src.ETL_DELETED=TRUE THEN DELETE
                    when matched then update set
                                target.WCO_EVENT=src.WCO_EVENT,
                                target.WCO_PK=src.WCO_PK,
                                target.WCO_ORG=src.WCO_ORG,
                                target.WCO_SCHEDULE_ITEM=src.WCO_SCHEDULE_ITEM,
                                target.WCO_TRANSID=src.WCO_TRANSID,
                                target.WCO_CONTRACTOR_QTY=src.WCO_CONTRACTOR_QTY,
                                target.WCO_CONTRACTOR_RATE=src.WCO_CONTRACTOR_RATE,
                                target.WCO_CHARGEDATE=src.WCO_CHARGEDATE,
                                target.WCO_COMMENTS=src.WCO_COMMENTS,
                                target.WCO_SCHEDITEM_DESC=src.WCO_SCHEDITEM_DESC,
                                target.WCO_SCHEDITEM_RATE=src.WCO_SCHEDITEM_RATE,
                                target.WCO_LINETOTAL=src.WCO_LINETOTAL,
                                target.WCO_WOSCHEDITEM=src.WCO_WOSCHEDITEM,
                                target.WCO_WOTYPE=src.WCO_WOTYPE,
                                target.WCO_WOPARENT=src.WCO_WOPARENT,
                                target.WCO_CONTRACT_CODE=src.WCO_CONTRACT_CODE,
                                target.WCO_CONTRACTOR=src.WCO_CONTRACTOR,
                                target.WCO_ACTIVITY=src.WCO_ACTIVITY,
                                target.WCO_ACTIVITY_DESC=src.WCO_ACTIVITY_DESC,
                                target.CREATEDBY=src.CREATEDBY,
                                target.CREATED=src.CREATED,
                                target.UPDATEDBY=src.UPDATEDBY,
                                target.UPDATED=src.UPDATED,
                                target.LASTSAVED=src.LASTSAVED,
                                target.UPDATECOUNT=src.UPDATECOUNT,
                                target.WCO_LINE_STATUS=src.WCO_LINE_STATUS,
                  target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                        target.etl_load_datetime=CURRENT_TIMESTAMP,
                      target.etl_load_metadatafilename=src.etl_load_metadatafilename 
  when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                                WCO_EVENT, 
                                WCO_PK, 
                                WCO_ORG, 
                                WCO_SCHEDULE_ITEM, 
                                WCO_TRANSID, 
                                WCO_CONTRACTOR_QTY, 
                                WCO_CONTRACTOR_RATE, 
                                WCO_CHARGEDATE, 
                                WCO_COMMENTS, 
                                WCO_SCHEDITEM_DESC, 
                                WCO_SCHEDITEM_RATE, 
                                WCO_LINETOTAL, 
                                WCO_WOSCHEDITEM, 
                                WCO_WOTYPE, 
                                WCO_WOPARENT, 
                                WCO_CONTRACT_CODE, 
                                WCO_CONTRACTOR, 
                                WCO_ACTIVITY, 
                                WCO_ACTIVITY_DESC, 
                                CREATEDBY, 
                                CREATED, 
                                UPDATEDBY, 
                                UPDATED, 
                                LASTSAVED, 
                                UPDATECOUNT, 
                                WCO_LINE_STATUS,
                      ETL_LASTSAVED,
                       
                        etl_load_datetime,
                      etl_load_metadatafilename) values (
                                 src.WCO_EVENT,
                                 src.WCO_PK,
                                 src.WCO_ORG,
                                 src.WCO_SCHEDULE_ITEM,
                                 src.WCO_TRANSID,
                                 src.WCO_CONTRACTOR_QTY,
                                 src.WCO_CONTRACTOR_RATE,
                                 src.WCO_CHARGEDATE,
                                 src.WCO_COMMENTS,
                                 src.WCO_SCHEDITEM_DESC,
                                 src.WCO_SCHEDITEM_RATE,
                                 src.WCO_LINETOTAL,
                                 src.WCO_WOSCHEDITEM,
                                 src.WCO_WOTYPE,
                                 src.WCO_WOPARENT,
                                 src.WCO_CONTRACT_CODE,
                                 src.WCO_CONTRACTOR,
                                 src.WCO_ACTIVITY,
                                 src.WCO_ACTIVITY_DESC,
                                 src.CREATEDBY,
                                 src.CREATED,
                                 src.UPDATEDBY,
                                 src.UPDATED,
                                 src.LASTSAVED,
                                 src.UPDATECOUNT,
                                 src.WCO_LINE_STATUS,
                       src.ETL_LASTSAVED,
                        CURRENT_TIMESTAMP,
                      src.etl_load_metadatafilename);`;    snowflake.execute( {sqlText: 
`
  merge into  "DATAHUB_TARGET_HISTORY"."EAM_DELETED_U5WOCLAIMS" as target using (
    
     
     SELECT * FROM (SELECT
    
    
                        strm.WCO_EVENT,
                        strm.WCO_PK,
                        strm.WCO_ORG,
                        strm.WCO_SCHEDULE_ITEM,
                        strm.WCO_TRANSID,
                        strm.WCO_CONTRACTOR_QTY,
                        strm.WCO_CONTRACTOR_RATE,
                        strm.WCO_CHARGEDATE,
                        strm.WCO_COMMENTS,
                        strm.WCO_SCHEDITEM_DESC,
                        strm.WCO_SCHEDITEM_RATE,
                        strm.WCO_LINETOTAL,
                        strm.WCO_WOSCHEDITEM,
                        strm.WCO_WOTYPE,
                        strm.WCO_WOPARENT,
                        strm.WCO_CONTRACT_CODE,
                        strm.WCO_CONTRACTOR,
                        strm.WCO_ACTIVITY,
                        strm.WCO_ACTIVITY_DESC,
                        strm.CREATEDBY,
                        strm.CREATED,
                        strm.UPDATEDBY,
                        strm.UPDATED,
                        strm.LASTSAVED,
                        strm.UPDATECOUNT,
                        strm.WCO_LINE_STATUS,
                        strm.ETL_LASTSAVED,
                        strm.ETL_DELETED,
                        strm.etl_load_datetime,
                      strm.etl_load_metadatafilename,
                      ROW_NUMBER() OVER (PARTITION BY 
                                        strm.WCO_EVENT,
                                                
                                        strm.WCO_PK,
                                                
                                        strm.WCO_ORG  ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                      
                     FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5WOCLAIMS as  strm
                     WHERE strm.ETL_DELETED=TRUE 
                     )   
                WHERE ROWNUMBER=1  
         
                     ) as src on
                                ((target.WCO_EVENT=src.WCO_EVENT) OR (target.WCO_EVENT IS NULL AND src.WCO_EVENT IS NULL)) and
                                                
                                ((target.WCO_PK=src.WCO_PK) OR (target.WCO_PK IS NULL AND src.WCO_PK IS NULL)) and
                                                
                                ((target.WCO_ORG=src.WCO_ORG) OR (target.WCO_ORG IS NULL AND src.WCO_ORG IS NULL))
                    when matched then update set
                                target.WCO_EVENT=src.WCO_EVENT,
                                target.WCO_PK=src.WCO_PK,
                                target.WCO_ORG=src.WCO_ORG,
                                target.WCO_SCHEDULE_ITEM=src.WCO_SCHEDULE_ITEM,
                                target.WCO_TRANSID=src.WCO_TRANSID,
                                target.WCO_CONTRACTOR_QTY=src.WCO_CONTRACTOR_QTY,
                                target.WCO_CONTRACTOR_RATE=src.WCO_CONTRACTOR_RATE,
                                target.WCO_CHARGEDATE=src.WCO_CHARGEDATE,
                                target.WCO_COMMENTS=src.WCO_COMMENTS,
                                target.WCO_SCHEDITEM_DESC=src.WCO_SCHEDITEM_DESC,
                                target.WCO_SCHEDITEM_RATE=src.WCO_SCHEDITEM_RATE,
                                target.WCO_LINETOTAL=src.WCO_LINETOTAL,
                                target.WCO_WOSCHEDITEM=src.WCO_WOSCHEDITEM,
                                target.WCO_WOTYPE=src.WCO_WOTYPE,
                                target.WCO_WOPARENT=src.WCO_WOPARENT,
                                target.WCO_CONTRACT_CODE=src.WCO_CONTRACT_CODE,
                                target.WCO_CONTRACTOR=src.WCO_CONTRACTOR,
                                target.WCO_ACTIVITY=src.WCO_ACTIVITY,
                                target.WCO_ACTIVITY_DESC=src.WCO_ACTIVITY_DESC,
                                target.CREATEDBY=src.CREATEDBY,
                                target.CREATED=src.CREATED,
                                target.UPDATEDBY=src.UPDATEDBY,
                                target.UPDATED=src.UPDATED,
                                target.LASTSAVED=src.LASTSAVED,
                                target.UPDATECOUNT=src.UPDATECOUNT,
                                target.WCO_LINE_STATUS=src.WCO_LINE_STATUS,
                  target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                        target.etl_load_datetime=CURRENT_TIMESTAMP,
                      target.etl_load_metadatafilename=src.etl_load_metadatafilename 
  when not matched then insert ( 
                                WCO_EVENT, 
                                WCO_PK, 
                                WCO_ORG, 
                                WCO_SCHEDULE_ITEM, 
                                WCO_TRANSID, 
                                WCO_CONTRACTOR_QTY, 
                                WCO_CONTRACTOR_RATE, 
                                WCO_CHARGEDATE, 
                                WCO_COMMENTS, 
                                WCO_SCHEDITEM_DESC, 
                                WCO_SCHEDITEM_RATE, 
                                WCO_LINETOTAL, 
                                WCO_WOSCHEDITEM, 
                                WCO_WOTYPE, 
                                WCO_WOPARENT, 
                                WCO_CONTRACT_CODE, 
                                WCO_CONTRACTOR, 
                                WCO_ACTIVITY, 
                                WCO_ACTIVITY_DESC, 
                                CREATEDBY, 
                                CREATED, 
                                UPDATEDBY, 
                                UPDATED, 
                                LASTSAVED, 
                                UPDATECOUNT, 
                                WCO_LINE_STATUS,
                      ETL_LASTSAVED,
                       
                        etl_load_datetime,
                      etl_load_metadatafilename,
etl_is_deleted,ETL_DELETED) values (
                                 src.WCO_EVENT,
                                 src.WCO_PK,
                                 src.WCO_ORG,
                                 src.WCO_SCHEDULE_ITEM,
                                 src.WCO_TRANSID,
                                 src.WCO_CONTRACTOR_QTY,
                                 src.WCO_CONTRACTOR_RATE,
                                 src.WCO_CHARGEDATE,
                                 src.WCO_COMMENTS,
                                 src.WCO_SCHEDITEM_DESC,
                                 src.WCO_SCHEDITEM_RATE,
                                 src.WCO_LINETOTAL,
                                 src.WCO_WOSCHEDITEM,
                                 src.WCO_WOTYPE,
                                 src.WCO_WOPARENT,
                                 src.WCO_CONTRACT_CODE,
                                 src.WCO_CONTRACTOR,
                                 src.WCO_ACTIVITY,
                                 src.WCO_ACTIVITY_DESC,
                                 src.CREATEDBY,
                                 src.CREATED,
                                 src.UPDATEDBY,
                                 src.UPDATED,
                                 src.LASTSAVED,
                                 src.UPDATECOUNT,
                                 src.WCO_LINE_STATUS,
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
