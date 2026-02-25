create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5MAILEVENTS()
    returns varchar not null
                language javascript
                as
                $$

//  Variables

var result = "";                                    // return status of this proc call
const process_name = Object.keys(this)[0];          // name of currently executing process
var number_of_rows_inserted = 0;                             // track number of rows we have inserted
var number_of_rows_updated = 0;                             // track number of rows we have updated


//  Step 1.

//  Start execution - log start

log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('Insert','" + process_name + "','Running','Started process execution.', 0,0,0);";
snowflake.execute({sqlText: log_sql_command});

snowflake.execute( {sqlText: "begin transaction;"} );
try
    {
        var sql_command = `
                            INSERT INTO LANDING_ERROR.EAM_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5MAILEVENTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5MAILEVENTS as target using (SELECT * FROM (SELECT 
            strm.MAE_ATTRIBPK, 
            strm.MAE_CODE, 
            strm.MAE_DATE, 
            strm.MAE_DOCPK, 
            strm.MAE_DOCRENTITY, 
            strm.MAE_EMAILERRCOUNT, 
            strm.MAE_EMAILRECIPIENT, 
            strm.MAE_ERROR, 
            strm.MAE_EWSURL, 
            strm.MAE_LASTSAVED, 
            strm.MAE_PARAM1, 
            strm.MAE_PARAM10, 
            strm.MAE_PARAM11, 
            strm.MAE_PARAM12, 
            strm.MAE_PARAM13, 
            strm.MAE_PARAM14, 
            strm.MAE_PARAM15, 
            strm.MAE_PARAM2, 
            strm.MAE_PARAM3, 
            strm.MAE_PARAM4, 
            strm.MAE_PARAM5, 
            strm.MAE_PARAM6, 
            strm.MAE_PARAM7, 
            strm.MAE_PARAM8, 
            strm.MAE_PARAM9, 
            strm.MAE_PTFERRCOUNT, 
            strm.MAE_PTFERROR, 
            strm.MAE_PTFSEND, 
            strm.MAE_RSTATUS, 
            strm.MAE_SEND, 
            strm.MAE_TEMPLATE, 
            strm.MAE_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MAE_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAILEVENTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MAE_CODE=src.MAE_CODE) OR (target.MAE_CODE IS NULL AND src.MAE_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.MAE_ATTRIBPK=src.MAE_ATTRIBPK, 
                    target.MAE_CODE=src.MAE_CODE, 
                    target.MAE_DATE=src.MAE_DATE, 
                    target.MAE_DOCPK=src.MAE_DOCPK, 
                    target.MAE_DOCRENTITY=src.MAE_DOCRENTITY, 
                    target.MAE_EMAILERRCOUNT=src.MAE_EMAILERRCOUNT, 
                    target.MAE_EMAILRECIPIENT=src.MAE_EMAILRECIPIENT, 
                    target.MAE_ERROR=src.MAE_ERROR, 
                    target.MAE_EWSURL=src.MAE_EWSURL, 
                    target.MAE_LASTSAVED=src.MAE_LASTSAVED, 
                    target.MAE_PARAM1=src.MAE_PARAM1, 
                    target.MAE_PARAM10=src.MAE_PARAM10, 
                    target.MAE_PARAM11=src.MAE_PARAM11, 
                    target.MAE_PARAM12=src.MAE_PARAM12, 
                    target.MAE_PARAM13=src.MAE_PARAM13, 
                    target.MAE_PARAM14=src.MAE_PARAM14, 
                    target.MAE_PARAM15=src.MAE_PARAM15, 
                    target.MAE_PARAM2=src.MAE_PARAM2, 
                    target.MAE_PARAM3=src.MAE_PARAM3, 
                    target.MAE_PARAM4=src.MAE_PARAM4, 
                    target.MAE_PARAM5=src.MAE_PARAM5, 
                    target.MAE_PARAM6=src.MAE_PARAM6, 
                    target.MAE_PARAM7=src.MAE_PARAM7, 
                    target.MAE_PARAM8=src.MAE_PARAM8, 
                    target.MAE_PARAM9=src.MAE_PARAM9, 
                    target.MAE_PTFERRCOUNT=src.MAE_PTFERRCOUNT, 
                    target.MAE_PTFERROR=src.MAE_PTFERROR, 
                    target.MAE_PTFSEND=src.MAE_PTFSEND, 
                    target.MAE_RSTATUS=src.MAE_RSTATUS, 
                    target.MAE_SEND=src.MAE_SEND, 
                    target.MAE_TEMPLATE=src.MAE_TEMPLATE, 
                    target.MAE_UPDATECOUNT=src.MAE_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    MAE_ATTRIBPK, 
                    MAE_CODE, 
                    MAE_DATE, 
                    MAE_DOCPK, 
                    MAE_DOCRENTITY, 
                    MAE_EMAILERRCOUNT, 
                    MAE_EMAILRECIPIENT, 
                    MAE_ERROR, 
                    MAE_EWSURL, 
                    MAE_LASTSAVED, 
                    MAE_PARAM1, 
                    MAE_PARAM10, 
                    MAE_PARAM11, 
                    MAE_PARAM12, 
                    MAE_PARAM13, 
                    MAE_PARAM14, 
                    MAE_PARAM15, 
                    MAE_PARAM2, 
                    MAE_PARAM3, 
                    MAE_PARAM4, 
                    MAE_PARAM5, 
                    MAE_PARAM6, 
                    MAE_PARAM7, 
                    MAE_PARAM8, 
                    MAE_PARAM9, 
                    MAE_PTFERRCOUNT, 
                    MAE_PTFERROR, 
                    MAE_PTFSEND, 
                    MAE_RSTATUS, 
                    MAE_SEND, 
                    MAE_TEMPLATE, 
                    MAE_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.MAE_ATTRIBPK, 
                    src.MAE_CODE, 
                    src.MAE_DATE, 
                    src.MAE_DOCPK, 
                    src.MAE_DOCRENTITY, 
                    src.MAE_EMAILERRCOUNT, 
                    src.MAE_EMAILRECIPIENT, 
                    src.MAE_ERROR, 
                    src.MAE_EWSURL, 
                    src.MAE_LASTSAVED, 
                    src.MAE_PARAM1, 
                    src.MAE_PARAM10, 
                    src.MAE_PARAM11, 
                    src.MAE_PARAM12, 
                    src.MAE_PARAM13, 
                    src.MAE_PARAM14, 
                    src.MAE_PARAM15, 
                    src.MAE_PARAM2, 
                    src.MAE_PARAM3, 
                    src.MAE_PARAM4, 
                    src.MAE_PARAM5, 
                    src.MAE_PARAM6, 
                    src.MAE_PARAM7, 
                    src.MAE_PARAM8, 
                    src.MAE_PARAM9, 
                    src.MAE_PTFERRCOUNT, 
                    src.MAE_PTFERROR, 
                    src.MAE_PTFSEND, 
                    src.MAE_RSTATUS, 
                    src.MAE_SEND, 
                    src.MAE_TEMPLATE, 
                    src.MAE_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5MAILEVENTS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.MAE_ATTRIBPK, 
            strm.MAE_CODE, 
            strm.MAE_DATE, 
            strm.MAE_DOCPK, 
            strm.MAE_DOCRENTITY, 
            strm.MAE_EMAILERRCOUNT, 
            strm.MAE_EMAILRECIPIENT, 
            strm.MAE_ERROR, 
            strm.MAE_EWSURL, 
            strm.MAE_LASTSAVED, 
            strm.MAE_PARAM1, 
            strm.MAE_PARAM10, 
            strm.MAE_PARAM11, 
            strm.MAE_PARAM12, 
            strm.MAE_PARAM13, 
            strm.MAE_PARAM14, 
            strm.MAE_PARAM15, 
            strm.MAE_PARAM2, 
            strm.MAE_PARAM3, 
            strm.MAE_PARAM4, 
            strm.MAE_PARAM5, 
            strm.MAE_PARAM6, 
            strm.MAE_PARAM7, 
            strm.MAE_PARAM8, 
            strm.MAE_PARAM9, 
            strm.MAE_PTFERRCOUNT, 
            strm.MAE_PTFERROR, 
            strm.MAE_PTFSEND, 
            strm.MAE_RSTATUS, 
            strm.MAE_SEND, 
            strm.MAE_TEMPLATE, 
            strm.MAE_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MAE_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAILEVENTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MAE_CODE=src.MAE_CODE) OR (target.MAE_CODE IS NULL AND src.MAE_CODE IS NULL)) 
                when matched then update set
                    target.MAE_ATTRIBPK=src.MAE_ATTRIBPK, 
                    target.MAE_CODE=src.MAE_CODE, 
                    target.MAE_DATE=src.MAE_DATE, 
                    target.MAE_DOCPK=src.MAE_DOCPK, 
                    target.MAE_DOCRENTITY=src.MAE_DOCRENTITY, 
                    target.MAE_EMAILERRCOUNT=src.MAE_EMAILERRCOUNT, 
                    target.MAE_EMAILRECIPIENT=src.MAE_EMAILRECIPIENT, 
                    target.MAE_ERROR=src.MAE_ERROR, 
                    target.MAE_EWSURL=src.MAE_EWSURL, 
                    target.MAE_LASTSAVED=src.MAE_LASTSAVED, 
                    target.MAE_PARAM1=src.MAE_PARAM1, 
                    target.MAE_PARAM10=src.MAE_PARAM10, 
                    target.MAE_PARAM11=src.MAE_PARAM11, 
                    target.MAE_PARAM12=src.MAE_PARAM12, 
                    target.MAE_PARAM13=src.MAE_PARAM13, 
                    target.MAE_PARAM14=src.MAE_PARAM14, 
                    target.MAE_PARAM15=src.MAE_PARAM15, 
                    target.MAE_PARAM2=src.MAE_PARAM2, 
                    target.MAE_PARAM3=src.MAE_PARAM3, 
                    target.MAE_PARAM4=src.MAE_PARAM4, 
                    target.MAE_PARAM5=src.MAE_PARAM5, 
                    target.MAE_PARAM6=src.MAE_PARAM6, 
                    target.MAE_PARAM7=src.MAE_PARAM7, 
                    target.MAE_PARAM8=src.MAE_PARAM8, 
                    target.MAE_PARAM9=src.MAE_PARAM9, 
                    target.MAE_PTFERRCOUNT=src.MAE_PTFERRCOUNT, 
                    target.MAE_PTFERROR=src.MAE_PTFERROR, 
                    target.MAE_PTFSEND=src.MAE_PTFSEND, 
                    target.MAE_RSTATUS=src.MAE_RSTATUS, 
                    target.MAE_SEND=src.MAE_SEND, 
                    target.MAE_TEMPLATE=src.MAE_TEMPLATE, 
                    target.MAE_UPDATECOUNT=src.MAE_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( MAE_ATTRIBPK, MAE_CODE, MAE_DATE, MAE_DOCPK, MAE_DOCRENTITY, MAE_EMAILERRCOUNT, MAE_EMAILRECIPIENT, MAE_ERROR, MAE_EWSURL, MAE_LASTSAVED, MAE_PARAM1, MAE_PARAM10, MAE_PARAM11, MAE_PARAM12, MAE_PARAM13, MAE_PARAM14, MAE_PARAM15, MAE_PARAM2, MAE_PARAM3, MAE_PARAM4, MAE_PARAM5, MAE_PARAM6, MAE_PARAM7, MAE_PARAM8, MAE_PARAM9, MAE_PTFERRCOUNT, MAE_PTFERROR, MAE_PTFSEND, MAE_RSTATUS, MAE_SEND, MAE_TEMPLATE, MAE_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.MAE_ATTRIBPK, 
                    src.MAE_CODE, 
                    src.MAE_DATE, 
                    src.MAE_DOCPK, 
                    src.MAE_DOCRENTITY, 
                    src.MAE_EMAILERRCOUNT, 
                    src.MAE_EMAILRECIPIENT, 
                    src.MAE_ERROR, 
                    src.MAE_EWSURL, 
                    src.MAE_LASTSAVED, 
                    src.MAE_PARAM1, 
                    src.MAE_PARAM10, 
                    src.MAE_PARAM11, 
                    src.MAE_PARAM12, 
                    src.MAE_PARAM13, 
                    src.MAE_PARAM14, 
                    src.MAE_PARAM15, 
                    src.MAE_PARAM2, 
                    src.MAE_PARAM3, 
                    src.MAE_PARAM4, 
                    src.MAE_PARAM5, 
                    src.MAE_PARAM6, 
                    src.MAE_PARAM7, 
                    src.MAE_PARAM8, 
                    src.MAE_PARAM9, 
                    src.MAE_PTFERRCOUNT, 
                    src.MAE_PTFERROR, 
                    src.MAE_PTFSEND, 
                    src.MAE_RSTATUS, 
                    src.MAE_SEND, 
                    src.MAE_TEMPLATE, 
                    src.MAE_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename,
                    case when ETL_DELETED=TRUE then TRUE else FALSE end,
                    ETL_DELETED);
    `
} );

                    
//  Get number of rows inserted
        
        var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
        var rs = sqlStmt.execute();
        var number_of_rows_inserted = sqlStmt.getNumRowsInserted();
        var number_of_rows_updated =  sqlStmt.getNumRowsUpdated();
                    

        
    snowflake.execute ({sqlText: "commit"});

    log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Success','Completed process execution.', 0 ," + number_of_rows_inserted.toString()+ "," +  number_of_rows_updated.toString() + ");";
    snowflake.execute({sqlText: log_sql_command});

    result = "Success"; 

    }       

    catch (err)  {
        snowflake.execute( {sqlText: "rollback;"} )
        var clean_error_msg = err.message.replace(/[^\w\s]/gi, '');
    //  Create a log entry to say INSERT STATEMENT failed
    
    log_sql_command = "call datahub_logging.sp_etl_log_ingestion_process('UpdateEnd','" + process_name + "','Failed','MERGE Failed. Error:" + err.code.toString()+" : "+ clean_error_msg  + "', 0 ,0,0);";
    snowflake.execute({sqlText: log_sql_command});
        ;
        throw "Failed: " + err.message;   // Return a success/error indicator.
        }
    return result;
    $$;