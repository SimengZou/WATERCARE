create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5MAILATTRIBS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5MAILATTRIBS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5MAILATTRIBS as target using (SELECT * FROM (SELECT 
            strm.MAA_ACTIVE, 
            strm.MAA_COMMENT, 
            strm.MAA_DELETE, 
            strm.MAA_ENTEREDBY, 
            strm.MAA_INCLUDEATTACHMENT, 
            strm.MAA_INCLUDEURL, 
            strm.MAA_INSERT, 
            strm.MAA_LASTSAVED, 
            strm.MAA_NEWSTATUS, 
            strm.MAA_OLDSTATUS, 
            strm.MAA_PK, 
            strm.MAA_TABLE, 
            strm.MAA_TEMPLATE, 
            strm.MAA_UPDATE, 
            strm.MAA_UPDATECOUNT, 
            strm.MAA_WORKFLOW, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MAA_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAILATTRIBS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MAA_PK=src.MAA_PK) OR (target.MAA_PK IS NULL AND src.MAA_PK IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.MAA_ACTIVE=src.MAA_ACTIVE, 
                    target.MAA_COMMENT=src.MAA_COMMENT, 
                    target.MAA_DELETE=src.MAA_DELETE, 
                    target.MAA_ENTEREDBY=src.MAA_ENTEREDBY, 
                    target.MAA_INCLUDEATTACHMENT=src.MAA_INCLUDEATTACHMENT, 
                    target.MAA_INCLUDEURL=src.MAA_INCLUDEURL, 
                    target.MAA_INSERT=src.MAA_INSERT, 
                    target.MAA_LASTSAVED=src.MAA_LASTSAVED, 
                    target.MAA_NEWSTATUS=src.MAA_NEWSTATUS, 
                    target.MAA_OLDSTATUS=src.MAA_OLDSTATUS, 
                    target.MAA_PK=src.MAA_PK, 
                    target.MAA_TABLE=src.MAA_TABLE, 
                    target.MAA_TEMPLATE=src.MAA_TEMPLATE, 
                    target.MAA_UPDATE=src.MAA_UPDATE, 
                    target.MAA_UPDATECOUNT=src.MAA_UPDATECOUNT, 
                    target.MAA_WORKFLOW=src.MAA_WORKFLOW, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    MAA_ACTIVE, 
                    MAA_COMMENT, 
                    MAA_DELETE, 
                    MAA_ENTEREDBY, 
                    MAA_INCLUDEATTACHMENT, 
                    MAA_INCLUDEURL, 
                    MAA_INSERT, 
                    MAA_LASTSAVED, 
                    MAA_NEWSTATUS, 
                    MAA_OLDSTATUS, 
                    MAA_PK, 
                    MAA_TABLE, 
                    MAA_TEMPLATE, 
                    MAA_UPDATE, 
                    MAA_UPDATECOUNT, 
                    MAA_WORKFLOW, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.MAA_ACTIVE, 
                    src.MAA_COMMENT, 
                    src.MAA_DELETE, 
                    src.MAA_ENTEREDBY, 
                    src.MAA_INCLUDEATTACHMENT, 
                    src.MAA_INCLUDEURL, 
                    src.MAA_INSERT, 
                    src.MAA_LASTSAVED, 
                    src.MAA_NEWSTATUS, 
                    src.MAA_OLDSTATUS, 
                    src.MAA_PK, 
                    src.MAA_TABLE, 
                    src.MAA_TEMPLATE, 
                    src.MAA_UPDATE, 
                    src.MAA_UPDATECOUNT, 
                    src.MAA_WORKFLOW, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5MAILATTRIBS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.MAA_ACTIVE, 
            strm.MAA_COMMENT, 
            strm.MAA_DELETE, 
            strm.MAA_ENTEREDBY, 
            strm.MAA_INCLUDEATTACHMENT, 
            strm.MAA_INCLUDEURL, 
            strm.MAA_INSERT, 
            strm.MAA_LASTSAVED, 
            strm.MAA_NEWSTATUS, 
            strm.MAA_OLDSTATUS, 
            strm.MAA_PK, 
            strm.MAA_TABLE, 
            strm.MAA_TEMPLATE, 
            strm.MAA_UPDATE, 
            strm.MAA_UPDATECOUNT, 
            strm.MAA_WORKFLOW, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.MAA_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5MAILATTRIBS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.MAA_PK=src.MAA_PK) OR (target.MAA_PK IS NULL AND src.MAA_PK IS NULL)) 
                when matched then update set
                    target.MAA_ACTIVE=src.MAA_ACTIVE, 
                    target.MAA_COMMENT=src.MAA_COMMENT, 
                    target.MAA_DELETE=src.MAA_DELETE, 
                    target.MAA_ENTEREDBY=src.MAA_ENTEREDBY, 
                    target.MAA_INCLUDEATTACHMENT=src.MAA_INCLUDEATTACHMENT, 
                    target.MAA_INCLUDEURL=src.MAA_INCLUDEURL, 
                    target.MAA_INSERT=src.MAA_INSERT, 
                    target.MAA_LASTSAVED=src.MAA_LASTSAVED, 
                    target.MAA_NEWSTATUS=src.MAA_NEWSTATUS, 
                    target.MAA_OLDSTATUS=src.MAA_OLDSTATUS, 
                    target.MAA_PK=src.MAA_PK, 
                    target.MAA_TABLE=src.MAA_TABLE, 
                    target.MAA_TEMPLATE=src.MAA_TEMPLATE, 
                    target.MAA_UPDATE=src.MAA_UPDATE, 
                    target.MAA_UPDATECOUNT=src.MAA_UPDATECOUNT, 
                    target.MAA_WORKFLOW=src.MAA_WORKFLOW, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( MAA_ACTIVE, MAA_COMMENT, MAA_DELETE, MAA_ENTEREDBY, MAA_INCLUDEATTACHMENT, MAA_INCLUDEURL, MAA_INSERT, MAA_LASTSAVED, MAA_NEWSTATUS, MAA_OLDSTATUS, MAA_PK, MAA_TABLE, MAA_TEMPLATE, MAA_UPDATE, MAA_UPDATECOUNT, MAA_WORKFLOW, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.MAA_ACTIVE, 
                    src.MAA_COMMENT, 
                    src.MAA_DELETE, 
                    src.MAA_ENTEREDBY, 
                    src.MAA_INCLUDEATTACHMENT, 
                    src.MAA_INCLUDEURL, 
                    src.MAA_INSERT, 
                    src.MAA_LASTSAVED, 
                    src.MAA_NEWSTATUS, 
                    src.MAA_OLDSTATUS, 
                    src.MAA_PK, 
                    src.MAA_TABLE, 
                    src.MAA_TEMPLATE, 
                    src.MAA_UPDATE, 
                    src.MAA_UPDATECOUNT, 
                    src.MAA_WORKFLOW, 
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