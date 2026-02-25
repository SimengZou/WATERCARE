create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5PROPERTYVALUES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5PROPERTYVALUES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5PROPERTYVALUES as target using (SELECT * FROM (SELECT 
            strm.PRV_CLASS, 
            strm.PRV_CLASS_ORG, 
            strm.PRV_CODE, 
            strm.PRV_CREATED, 
            strm.PRV_DVALUE, 
            strm.PRV_LASTSAVED, 
            strm.PRV_NOTUSED, 
            strm.PRV_NVALUE, 
            strm.PRV_PROPERTY, 
            strm.PRV_RENTITY, 
            strm.PRV_SEQNO, 
            strm.PRV_UPDATECOUNT, 
            strm.PRV_UPDATED, 
            strm.PRV_VALUE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PRV_PROPERTY,
            strm.PRV_RENTITY,
            strm.PRV_CLASS,
            strm.PRV_CLASS_ORG,
            strm.PRV_CODE,
            strm.PRV_SEQNO ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PROPERTYVALUES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PRV_PROPERTY=src.PRV_PROPERTY) OR (target.PRV_PROPERTY IS NULL AND src.PRV_PROPERTY IS NULL)) AND
            ((target.PRV_RENTITY=src.PRV_RENTITY) OR (target.PRV_RENTITY IS NULL AND src.PRV_RENTITY IS NULL)) AND
            ((target.PRV_CLASS=src.PRV_CLASS) OR (target.PRV_CLASS IS NULL AND src.PRV_CLASS IS NULL)) AND
            ((target.PRV_CLASS_ORG=src.PRV_CLASS_ORG) OR (target.PRV_CLASS_ORG IS NULL AND src.PRV_CLASS_ORG IS NULL)) AND
            ((target.PRV_CODE=src.PRV_CODE) OR (target.PRV_CODE IS NULL AND src.PRV_CODE IS NULL)) AND
            ((target.PRV_SEQNO=src.PRV_SEQNO) OR (target.PRV_SEQNO IS NULL AND src.PRV_SEQNO IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.PRV_CLASS=src.PRV_CLASS, 
                    target.PRV_CLASS_ORG=src.PRV_CLASS_ORG, 
                    target.PRV_CODE=src.PRV_CODE, 
                    target.PRV_CREATED=src.PRV_CREATED, 
                    target.PRV_DVALUE=src.PRV_DVALUE, 
                    target.PRV_LASTSAVED=src.PRV_LASTSAVED, 
                    target.PRV_NOTUSED=src.PRV_NOTUSED, 
                    target.PRV_NVALUE=src.PRV_NVALUE, 
                    target.PRV_PROPERTY=src.PRV_PROPERTY, 
                    target.PRV_RENTITY=src.PRV_RENTITY, 
                    target.PRV_SEQNO=src.PRV_SEQNO, 
                    target.PRV_UPDATECOUNT=src.PRV_UPDATECOUNT, 
                    target.PRV_UPDATED=src.PRV_UPDATED, 
                    target.PRV_VALUE=src.PRV_VALUE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    PRV_CLASS, 
                    PRV_CLASS_ORG, 
                    PRV_CODE, 
                    PRV_CREATED, 
                    PRV_DVALUE, 
                    PRV_LASTSAVED, 
                    PRV_NOTUSED, 
                    PRV_NVALUE, 
                    PRV_PROPERTY, 
                    PRV_RENTITY, 
                    PRV_SEQNO, 
                    PRV_UPDATECOUNT, 
                    PRV_UPDATED, 
                    PRV_VALUE, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.PRV_CLASS, 
                    src.PRV_CLASS_ORG, 
                    src.PRV_CODE, 
                    src.PRV_CREATED, 
                    src.PRV_DVALUE, 
                    src.PRV_LASTSAVED, 
                    src.PRV_NOTUSED, 
                    src.PRV_NVALUE, 
                    src.PRV_PROPERTY, 
                    src.PRV_RENTITY, 
                    src.PRV_SEQNO, 
                    src.PRV_UPDATECOUNT, 
                    src.PRV_UPDATED, 
                    src.PRV_VALUE, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5PROPERTYVALUES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.PRV_CLASS, 
            strm.PRV_CLASS_ORG, 
            strm.PRV_CODE, 
            strm.PRV_CREATED, 
            strm.PRV_DVALUE, 
            strm.PRV_LASTSAVED, 
            strm.PRV_NOTUSED, 
            strm.PRV_NVALUE, 
            strm.PRV_PROPERTY, 
            strm.PRV_RENTITY, 
            strm.PRV_SEQNO, 
            strm.PRV_UPDATECOUNT, 
            strm.PRV_UPDATED, 
            strm.PRV_VALUE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PRV_PROPERTY,
            strm.PRV_RENTITY,
            strm.PRV_CLASS,
            strm.PRV_CLASS_ORG,
            strm.PRV_CODE,
            strm.PRV_SEQNO ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PROPERTYVALUES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PRV_PROPERTY=src.PRV_PROPERTY) OR (target.PRV_PROPERTY IS NULL AND src.PRV_PROPERTY IS NULL)) AND
            ((target.PRV_RENTITY=src.PRV_RENTITY) OR (target.PRV_RENTITY IS NULL AND src.PRV_RENTITY IS NULL)) AND
            ((target.PRV_CLASS=src.PRV_CLASS) OR (target.PRV_CLASS IS NULL AND src.PRV_CLASS IS NULL)) AND
            ((target.PRV_CLASS_ORG=src.PRV_CLASS_ORG) OR (target.PRV_CLASS_ORG IS NULL AND src.PRV_CLASS_ORG IS NULL)) AND
            ((target.PRV_CODE=src.PRV_CODE) OR (target.PRV_CODE IS NULL AND src.PRV_CODE IS NULL)) AND
            ((target.PRV_SEQNO=src.PRV_SEQNO) OR (target.PRV_SEQNO IS NULL AND src.PRV_SEQNO IS NULL)) 
                when matched then update set
                    target.PRV_CLASS=src.PRV_CLASS, 
                    target.PRV_CLASS_ORG=src.PRV_CLASS_ORG, 
                    target.PRV_CODE=src.PRV_CODE, 
                    target.PRV_CREATED=src.PRV_CREATED, 
                    target.PRV_DVALUE=src.PRV_DVALUE, 
                    target.PRV_LASTSAVED=src.PRV_LASTSAVED, 
                    target.PRV_NOTUSED=src.PRV_NOTUSED, 
                    target.PRV_NVALUE=src.PRV_NVALUE, 
                    target.PRV_PROPERTY=src.PRV_PROPERTY, 
                    target.PRV_RENTITY=src.PRV_RENTITY, 
                    target.PRV_SEQNO=src.PRV_SEQNO, 
                    target.PRV_UPDATECOUNT=src.PRV_UPDATECOUNT, 
                    target.PRV_UPDATED=src.PRV_UPDATED, 
                    target.PRV_VALUE=src.PRV_VALUE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( PRV_CLASS, PRV_CLASS_ORG, PRV_CODE, PRV_CREATED, PRV_DVALUE, PRV_LASTSAVED, PRV_NOTUSED, PRV_NVALUE, PRV_PROPERTY, PRV_RENTITY, PRV_SEQNO, PRV_UPDATECOUNT, PRV_UPDATED, PRV_VALUE, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.PRV_CLASS, 
                    src.PRV_CLASS_ORG, 
                    src.PRV_CODE, 
                    src.PRV_CREATED, 
                    src.PRV_DVALUE, 
                    src.PRV_LASTSAVED, 
                    src.PRV_NOTUSED, 
                    src.PRV_NVALUE, 
                    src.PRV_PROPERTY, 
                    src.PRV_RENTITY, 
                    src.PRV_SEQNO, 
                    src.PRV_UPDATECOUNT, 
                    src.PRV_UPDATED, 
                    src.PRV_VALUE, 
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