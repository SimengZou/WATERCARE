
                    
                create or replace procedure DATAHUB_INTEGRATION.EAM_TARGET_MERGE_U5CLAIMCONFIG()
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
                                merge into  "DATAHUB_TARGET"."EAM_U5CLAIMCONFIG" as target using (
                                  
                      
                  
                    SELECT * FROM (SELECT
                    
                    
                        strm.CLC_ORG,
                        strm.CLC_CONTRACTOR,
                        strm.CLC_COSTITEM,
                        strm.CLC_LNCOMPANY,
                        strm.CLC_STGCOSTCODE,
                        strm.CLC_PROJECTNUMBER,
                        strm.CLC_PROJECTACTIVITY,
                        strm.CLC_SUPPLIER,
                        strm.CLC_STORE,
                        strm.CLC_CREATOR,
                        strm.CLC_REQUESTOR,
                        strm.CLC_REQUESTOR2,
                        strm.CLC_PURCHASEOFFICE,
                        strm.CLC_CONTORDERTYPE,
                        strm.CLC_NONCONTORDERTYPE,
                        strm.CLC_ORDERSERIES,
                        strm.CLC_EXCLUDEWOFROMLN,
                        strm.CLC_EXCLUDEPOFROMLN,
                        strm.CLC_SPLITINTERCOMPANYCOSTS,
                        strm.CREATEDBY,
                        strm.CREATED,
                        strm.UPDATEDBY,
                        strm.UPDATED,
                        strm.LASTSAVED,
                        strm.UPDATECOUNT,
                        strm.CLC_PMWORATEAPP,
                        strm.CLC_CHILDWORATEAPP,
                        strm.ETL_LASTSAVED,
                        strm.ETL_DELETED,
                        strm.etl_load_datetime,
                      strm.etl_load_metadatafilename,
                      ROW_NUMBER() OVER (PARTITION BY 
                                        strm.CLC_ORG,
                                                
                                        strm.CLC_CONTRACTOR  ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                      
                     FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5CLAIMCONFIG as  strm
                     )   
                WHERE ROWNUMBER=1  
               
                     ) as src on
                                ((target.CLC_ORG=src.CLC_ORG) OR (target.CLC_ORG IS NULL AND src.CLC_ORG IS NULL)) and
                                                
                                ((target.CLC_CONTRACTOR=src.CLC_CONTRACTOR) OR (target.CLC_CONTRACTOR IS NULL AND src.CLC_CONTRACTOR IS NULL))
                    when matched and src.ETL_DELETED=TRUE THEN DELETE
                    when matched then update set
                                target.CLC_ORG=src.CLC_ORG,
                                target.CLC_CONTRACTOR=src.CLC_CONTRACTOR,
                                target.CLC_COSTITEM=src.CLC_COSTITEM,
                                target.CLC_LNCOMPANY=src.CLC_LNCOMPANY,
                                target.CLC_STGCOSTCODE=src.CLC_STGCOSTCODE,
                                target.CLC_PROJECTNUMBER=src.CLC_PROJECTNUMBER,
                                target.CLC_PROJECTACTIVITY=src.CLC_PROJECTACTIVITY,
                                target.CLC_SUPPLIER=src.CLC_SUPPLIER,
                                target.CLC_STORE=src.CLC_STORE,
                                target.CLC_CREATOR=src.CLC_CREATOR,
                                target.CLC_REQUESTOR=src.CLC_REQUESTOR,
                                target.CLC_REQUESTOR2=src.CLC_REQUESTOR2,
                                target.CLC_PURCHASEOFFICE=src.CLC_PURCHASEOFFICE,
                                target.CLC_CONTORDERTYPE=src.CLC_CONTORDERTYPE,
                                target.CLC_NONCONTORDERTYPE=src.CLC_NONCONTORDERTYPE,
                                target.CLC_ORDERSERIES=src.CLC_ORDERSERIES,
                                target.CLC_EXCLUDEWOFROMLN=src.CLC_EXCLUDEWOFROMLN,
                                target.CLC_EXCLUDEPOFROMLN=src.CLC_EXCLUDEPOFROMLN,
                                target.CLC_SPLITINTERCOMPANYCOSTS=src.CLC_SPLITINTERCOMPANYCOSTS,
                                target.CREATEDBY=src.CREATEDBY,
                                target.CREATED=src.CREATED,
                                target.UPDATEDBY=src.UPDATEDBY,
                                target.UPDATED=src.UPDATED,
                                target.LASTSAVED=src.LASTSAVED,
                                target.UPDATECOUNT=src.UPDATECOUNT,
                                target.CLC_PMWORATEAPP=src.CLC_PMWORATEAPP,
                                target.CLC_CHILDWORATEAPP=src.CLC_CHILDWORATEAPP,
                  target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                        target.etl_load_datetime=CURRENT_TIMESTAMP,
                      target.etl_load_metadatafilename=src.etl_load_metadatafilename 
  when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                                CLC_ORG, 
                                CLC_CONTRACTOR, 
                                CLC_COSTITEM, 
                                CLC_LNCOMPANY, 
                                CLC_STGCOSTCODE, 
                                CLC_PROJECTNUMBER, 
                                CLC_PROJECTACTIVITY, 
                                CLC_SUPPLIER, 
                                CLC_STORE, 
                                CLC_CREATOR, 
                                CLC_REQUESTOR, 
                                CLC_REQUESTOR2, 
                                CLC_PURCHASEOFFICE, 
                                CLC_CONTORDERTYPE, 
                                CLC_NONCONTORDERTYPE, 
                                CLC_ORDERSERIES, 
                                CLC_EXCLUDEWOFROMLN, 
                                CLC_EXCLUDEPOFROMLN, 
                                CLC_SPLITINTERCOMPANYCOSTS, 
                                CREATEDBY, 
                                CREATED, 
                                UPDATEDBY, 
                                UPDATED, 
                                LASTSAVED, 
                                UPDATECOUNT, 
                                CLC_PMWORATEAPP, 
                                CLC_CHILDWORATEAPP,
                      ETL_LASTSAVED,
                       
                        etl_load_datetime,
                      etl_load_metadatafilename) values (
                                 src.CLC_ORG,
                                 src.CLC_CONTRACTOR,
                                 src.CLC_COSTITEM,
                                 src.CLC_LNCOMPANY,
                                 src.CLC_STGCOSTCODE,
                                 src.CLC_PROJECTNUMBER,
                                 src.CLC_PROJECTACTIVITY,
                                 src.CLC_SUPPLIER,
                                 src.CLC_STORE,
                                 src.CLC_CREATOR,
                                 src.CLC_REQUESTOR,
                                 src.CLC_REQUESTOR2,
                                 src.CLC_PURCHASEOFFICE,
                                 src.CLC_CONTORDERTYPE,
                                 src.CLC_NONCONTORDERTYPE,
                                 src.CLC_ORDERSERIES,
                                 src.CLC_EXCLUDEWOFROMLN,
                                 src.CLC_EXCLUDEPOFROMLN,
                                 src.CLC_SPLITINTERCOMPANYCOSTS,
                                 src.CREATEDBY,
                                 src.CREATED,
                                 src.UPDATEDBY,
                                 src.UPDATED,
                                 src.LASTSAVED,
                                 src.UPDATECOUNT,
                                 src.CLC_PMWORATEAPP,
                                 src.CLC_CHILDWORATEAPP,
                       src.ETL_LASTSAVED,
                        CURRENT_TIMESTAMP,
                      src.etl_load_metadatafilename);`;    snowflake.execute( {sqlText: 
`
  merge into  "DATAHUB_TARGET_HISTORY"."EAM_DELETED_U5CLAIMCONFIG" as target using (
    
     
     SELECT * FROM (SELECT
    
    
                        strm.CLC_ORG,
                        strm.CLC_CONTRACTOR,
                        strm.CLC_COSTITEM,
                        strm.CLC_LNCOMPANY,
                        strm.CLC_STGCOSTCODE,
                        strm.CLC_PROJECTNUMBER,
                        strm.CLC_PROJECTACTIVITY,
                        strm.CLC_SUPPLIER,
                        strm.CLC_STORE,
                        strm.CLC_CREATOR,
                        strm.CLC_REQUESTOR,
                        strm.CLC_REQUESTOR2,
                        strm.CLC_PURCHASEOFFICE,
                        strm.CLC_CONTORDERTYPE,
                        strm.CLC_NONCONTORDERTYPE,
                        strm.CLC_ORDERSERIES,
                        strm.CLC_EXCLUDEWOFROMLN,
                        strm.CLC_EXCLUDEPOFROMLN,
                        strm.CLC_SPLITINTERCOMPANYCOSTS,
                        strm.CREATEDBY,
                        strm.CREATED,
                        strm.UPDATEDBY,
                        strm.UPDATED,
                        strm.LASTSAVED,
                        strm.UPDATECOUNT,
                        strm.CLC_PMWORATEAPP,
                        strm.CLC_CHILDWORATEAPP,
                        strm.ETL_LASTSAVED,
                        strm.ETL_DELETED,
                        strm.etl_load_datetime,
                      strm.etl_load_metadatafilename,
                      ROW_NUMBER() OVER (PARTITION BY 
                                        strm.CLC_ORG,
                                                
                                        strm.CLC_CONTRACTOR  ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                      
                     FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_U5CLAIMCONFIG as  strm
                     WHERE strm.ETL_DELETED=TRUE 
                     )   
                WHERE ROWNUMBER=1  
         
                     ) as src on
                                ((target.CLC_ORG=src.CLC_ORG) OR (target.CLC_ORG IS NULL AND src.CLC_ORG IS NULL)) and
                                                
                                ((target.CLC_CONTRACTOR=src.CLC_CONTRACTOR) OR (target.CLC_CONTRACTOR IS NULL AND src.CLC_CONTRACTOR IS NULL))
                    when matched then update set
                                target.CLC_ORG=src.CLC_ORG,
                                target.CLC_CONTRACTOR=src.CLC_CONTRACTOR,
                                target.CLC_COSTITEM=src.CLC_COSTITEM,
                                target.CLC_LNCOMPANY=src.CLC_LNCOMPANY,
                                target.CLC_STGCOSTCODE=src.CLC_STGCOSTCODE,
                                target.CLC_PROJECTNUMBER=src.CLC_PROJECTNUMBER,
                                target.CLC_PROJECTACTIVITY=src.CLC_PROJECTACTIVITY,
                                target.CLC_SUPPLIER=src.CLC_SUPPLIER,
                                target.CLC_STORE=src.CLC_STORE,
                                target.CLC_CREATOR=src.CLC_CREATOR,
                                target.CLC_REQUESTOR=src.CLC_REQUESTOR,
                                target.CLC_REQUESTOR2=src.CLC_REQUESTOR2,
                                target.CLC_PURCHASEOFFICE=src.CLC_PURCHASEOFFICE,
                                target.CLC_CONTORDERTYPE=src.CLC_CONTORDERTYPE,
                                target.CLC_NONCONTORDERTYPE=src.CLC_NONCONTORDERTYPE,
                                target.CLC_ORDERSERIES=src.CLC_ORDERSERIES,
                                target.CLC_EXCLUDEWOFROMLN=src.CLC_EXCLUDEWOFROMLN,
                                target.CLC_EXCLUDEPOFROMLN=src.CLC_EXCLUDEPOFROMLN,
                                target.CLC_SPLITINTERCOMPANYCOSTS=src.CLC_SPLITINTERCOMPANYCOSTS,
                                target.CREATEDBY=src.CREATEDBY,
                                target.CREATED=src.CREATED,
                                target.UPDATEDBY=src.UPDATEDBY,
                                target.UPDATED=src.UPDATED,
                                target.LASTSAVED=src.LASTSAVED,
                                target.UPDATECOUNT=src.UPDATECOUNT,
                                target.CLC_PMWORATEAPP=src.CLC_PMWORATEAPP,
                                target.CLC_CHILDWORATEAPP=src.CLC_CHILDWORATEAPP,
                  target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                        target.etl_load_datetime=CURRENT_TIMESTAMP,
                      target.etl_load_metadatafilename=src.etl_load_metadatafilename 
  when not matched then insert ( 
                                CLC_ORG, 
                                CLC_CONTRACTOR, 
                                CLC_COSTITEM, 
                                CLC_LNCOMPANY, 
                                CLC_STGCOSTCODE, 
                                CLC_PROJECTNUMBER, 
                                CLC_PROJECTACTIVITY, 
                                CLC_SUPPLIER, 
                                CLC_STORE, 
                                CLC_CREATOR, 
                                CLC_REQUESTOR, 
                                CLC_REQUESTOR2, 
                                CLC_PURCHASEOFFICE, 
                                CLC_CONTORDERTYPE, 
                                CLC_NONCONTORDERTYPE, 
                                CLC_ORDERSERIES, 
                                CLC_EXCLUDEWOFROMLN, 
                                CLC_EXCLUDEPOFROMLN, 
                                CLC_SPLITINTERCOMPANYCOSTS, 
                                CREATEDBY, 
                                CREATED, 
                                UPDATEDBY, 
                                UPDATED, 
                                LASTSAVED, 
                                UPDATECOUNT, 
                                CLC_PMWORATEAPP, 
                                CLC_CHILDWORATEAPP,
                      ETL_LASTSAVED,
                       
                        etl_load_datetime,
                      etl_load_metadatafilename,
etl_is_deleted,ETL_DELETED) values (
                                 src.CLC_ORG,
                                 src.CLC_CONTRACTOR,
                                 src.CLC_COSTITEM,
                                 src.CLC_LNCOMPANY,
                                 src.CLC_STGCOSTCODE,
                                 src.CLC_PROJECTNUMBER,
                                 src.CLC_PROJECTACTIVITY,
                                 src.CLC_SUPPLIER,
                                 src.CLC_STORE,
                                 src.CLC_CREATOR,
                                 src.CLC_REQUESTOR,
                                 src.CLC_REQUESTOR2,
                                 src.CLC_PURCHASEOFFICE,
                                 src.CLC_CONTORDERTYPE,
                                 src.CLC_NONCONTORDERTYPE,
                                 src.CLC_ORDERSERIES,
                                 src.CLC_EXCLUDEWOFROMLN,
                                 src.CLC_EXCLUDEPOFROMLN,
                                 src.CLC_SPLITINTERCOMPANYCOSTS,
                                 src.CREATEDBY,
                                 src.CREATED,
                                 src.UPDATEDBY,
                                 src.UPDATED,
                                 src.LASTSAVED,
                                 src.UPDATECOUNT,
                                 src.CLC_PMWORATEAPP,
                                 src.CLC_CHILDWORATEAPP,
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
