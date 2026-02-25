create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ADDRESS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ADDRESS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ADDRESS as target using (SELECT * FROM (SELECT 
            strm.ADR_ADDRESS1, 
            strm.ADR_ADDRESS2, 
            strm.ADR_ADDRESS3, 
            strm.ADR_CITY, 
            strm.ADR_CODE, 
            strm.ADR_COUNTRY, 
            strm.ADR_EMAIL, 
            strm.ADR_FAX, 
            strm.ADR_LASTSAVED, 
            strm.ADR_PHONE, 
            strm.ADR_PHONEEXTN, 
            strm.ADR_RENTITY, 
            strm.ADR_RTYPE, 
            strm.ADR_STATE, 
            strm.ADR_TEXT, 
            strm.ADR_UPDATECOUNT, 
            strm.ADR_ZIP, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ADR_CODE,
            strm.ADR_RENTITY,
            strm.ADR_RTYPE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ADDRESS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ADR_CODE=src.ADR_CODE) OR (target.ADR_CODE IS NULL AND src.ADR_CODE IS NULL)) AND
            ((target.ADR_RENTITY=src.ADR_RENTITY) OR (target.ADR_RENTITY IS NULL AND src.ADR_RENTITY IS NULL)) AND
            ((target.ADR_RTYPE=src.ADR_RTYPE) OR (target.ADR_RTYPE IS NULL AND src.ADR_RTYPE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADR_ADDRESS1=src.ADR_ADDRESS1, 
                    target.ADR_ADDRESS2=src.ADR_ADDRESS2, 
                    target.ADR_ADDRESS3=src.ADR_ADDRESS3, 
                    target.ADR_CITY=src.ADR_CITY, 
                    target.ADR_CODE=src.ADR_CODE, 
                    target.ADR_COUNTRY=src.ADR_COUNTRY, 
                    target.ADR_EMAIL=src.ADR_EMAIL, 
                    target.ADR_FAX=src.ADR_FAX, 
                    target.ADR_LASTSAVED=src.ADR_LASTSAVED, 
                    target.ADR_PHONE=src.ADR_PHONE, 
                    target.ADR_PHONEEXTN=src.ADR_PHONEEXTN, 
                    target.ADR_RENTITY=src.ADR_RENTITY, 
                    target.ADR_RTYPE=src.ADR_RTYPE, 
                    target.ADR_STATE=src.ADR_STATE, 
                    target.ADR_TEXT=src.ADR_TEXT, 
                    target.ADR_UPDATECOUNT=src.ADR_UPDATECOUNT, 
                    target.ADR_ZIP=src.ADR_ZIP, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADR_ADDRESS1, 
                    ADR_ADDRESS2, 
                    ADR_ADDRESS3, 
                    ADR_CITY, 
                    ADR_CODE, 
                    ADR_COUNTRY, 
                    ADR_EMAIL, 
                    ADR_FAX, 
                    ADR_LASTSAVED, 
                    ADR_PHONE, 
                    ADR_PHONEEXTN, 
                    ADR_RENTITY, 
                    ADR_RTYPE, 
                    ADR_STATE, 
                    ADR_TEXT, 
                    ADR_UPDATECOUNT, 
                    ADR_ZIP, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADR_ADDRESS1, 
                    src.ADR_ADDRESS2, 
                    src.ADR_ADDRESS3, 
                    src.ADR_CITY, 
                    src.ADR_CODE, 
                    src.ADR_COUNTRY, 
                    src.ADR_EMAIL, 
                    src.ADR_FAX, 
                    src.ADR_LASTSAVED, 
                    src.ADR_PHONE, 
                    src.ADR_PHONEEXTN, 
                    src.ADR_RENTITY, 
                    src.ADR_RTYPE, 
                    src.ADR_STATE, 
                    src.ADR_TEXT, 
                    src.ADR_UPDATECOUNT, 
                    src.ADR_ZIP, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ADDRESS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ADR_ADDRESS1, 
            strm.ADR_ADDRESS2, 
            strm.ADR_ADDRESS3, 
            strm.ADR_CITY, 
            strm.ADR_CODE, 
            strm.ADR_COUNTRY, 
            strm.ADR_EMAIL, 
            strm.ADR_FAX, 
            strm.ADR_LASTSAVED, 
            strm.ADR_PHONE, 
            strm.ADR_PHONEEXTN, 
            strm.ADR_RENTITY, 
            strm.ADR_RTYPE, 
            strm.ADR_STATE, 
            strm.ADR_TEXT, 
            strm.ADR_UPDATECOUNT, 
            strm.ADR_ZIP, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ADR_CODE,
            strm.ADR_RENTITY,
            strm.ADR_RTYPE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ADDRESS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ADR_CODE=src.ADR_CODE) OR (target.ADR_CODE IS NULL AND src.ADR_CODE IS NULL)) AND
            ((target.ADR_RENTITY=src.ADR_RENTITY) OR (target.ADR_RENTITY IS NULL AND src.ADR_RENTITY IS NULL)) AND
            ((target.ADR_RTYPE=src.ADR_RTYPE) OR (target.ADR_RTYPE IS NULL AND src.ADR_RTYPE IS NULL)) 
                when matched then update set
                    target.ADR_ADDRESS1=src.ADR_ADDRESS1, 
                    target.ADR_ADDRESS2=src.ADR_ADDRESS2, 
                    target.ADR_ADDRESS3=src.ADR_ADDRESS3, 
                    target.ADR_CITY=src.ADR_CITY, 
                    target.ADR_CODE=src.ADR_CODE, 
                    target.ADR_COUNTRY=src.ADR_COUNTRY, 
                    target.ADR_EMAIL=src.ADR_EMAIL, 
                    target.ADR_FAX=src.ADR_FAX, 
                    target.ADR_LASTSAVED=src.ADR_LASTSAVED, 
                    target.ADR_PHONE=src.ADR_PHONE, 
                    target.ADR_PHONEEXTN=src.ADR_PHONEEXTN, 
                    target.ADR_RENTITY=src.ADR_RENTITY, 
                    target.ADR_RTYPE=src.ADR_RTYPE, 
                    target.ADR_STATE=src.ADR_STATE, 
                    target.ADR_TEXT=src.ADR_TEXT, 
                    target.ADR_UPDATECOUNT=src.ADR_UPDATECOUNT, 
                    target.ADR_ZIP=src.ADR_ZIP, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADR_ADDRESS1, ADR_ADDRESS2, ADR_ADDRESS3, ADR_CITY, ADR_CODE, ADR_COUNTRY, ADR_EMAIL, ADR_FAX, ADR_LASTSAVED, ADR_PHONE, ADR_PHONEEXTN, ADR_RENTITY, ADR_RTYPE, ADR_STATE, ADR_TEXT, ADR_UPDATECOUNT, ADR_ZIP, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADR_ADDRESS1, 
                    src.ADR_ADDRESS2, 
                    src.ADR_ADDRESS3, 
                    src.ADR_CITY, 
                    src.ADR_CODE, 
                    src.ADR_COUNTRY, 
                    src.ADR_EMAIL, 
                    src.ADR_FAX, 
                    src.ADR_LASTSAVED, 
                    src.ADR_PHONE, 
                    src.ADR_PHONEEXTN, 
                    src.ADR_RENTITY, 
                    src.ADR_RTYPE, 
                    src.ADR_STATE, 
                    src.ADR_TEXT, 
                    src.ADR_UPDATECOUNT, 
                    src.ADR_ZIP, 
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