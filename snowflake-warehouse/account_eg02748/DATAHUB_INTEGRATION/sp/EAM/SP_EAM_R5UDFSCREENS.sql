create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5UDFSCREENS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5UDFSCREENS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5UDFSCREENS as target using (SELECT * FROM (SELECT 
            strm.USC_ALLOWCOMMENTS, 
            strm.USC_ALLOWDOCUMENTS, 
            strm.USC_AUTOGENERATEKEY, 
            strm.USC_CLASS, 
            strm.USC_CREATED, 
            strm.USC_CREATEDBY, 
            strm.USC_DESC, 
            strm.USC_ENTITY, 
            strm.USC_GENERATED, 
            strm.USC_ISTAB, 
            strm.USC_LASTSAVED, 
            strm.USC_NOTUSED, 
            strm.USC_ORGSECURITY, 
            strm.USC_PARENTSCREEN, 
            strm.USC_SCREENNAME, 
            strm.USC_STATUSENTITY, 
            strm.USC_TABLENAME, 
            strm.USC_TYPEENTITY, 
            strm.USC_UPDATECOUNT, 
            strm.USC_UPDATED, 
            strm.USC_UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.USC_SCREENNAME ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5UDFSCREENS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.USC_SCREENNAME=src.USC_SCREENNAME) OR (target.USC_SCREENNAME IS NULL AND src.USC_SCREENNAME IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.USC_ALLOWCOMMENTS=src.USC_ALLOWCOMMENTS, 
                    target.USC_ALLOWDOCUMENTS=src.USC_ALLOWDOCUMENTS, 
                    target.USC_AUTOGENERATEKEY=src.USC_AUTOGENERATEKEY, 
                    target.USC_CLASS=src.USC_CLASS, 
                    target.USC_CREATED=src.USC_CREATED, 
                    target.USC_CREATEDBY=src.USC_CREATEDBY, 
                    target.USC_DESC=src.USC_DESC, 
                    target.USC_ENTITY=src.USC_ENTITY, 
                    target.USC_GENERATED=src.USC_GENERATED, 
                    target.USC_ISTAB=src.USC_ISTAB, 
                    target.USC_LASTSAVED=src.USC_LASTSAVED, 
                    target.USC_NOTUSED=src.USC_NOTUSED, 
                    target.USC_ORGSECURITY=src.USC_ORGSECURITY, 
                    target.USC_PARENTSCREEN=src.USC_PARENTSCREEN, 
                    target.USC_SCREENNAME=src.USC_SCREENNAME, 
                    target.USC_STATUSENTITY=src.USC_STATUSENTITY, 
                    target.USC_TABLENAME=src.USC_TABLENAME, 
                    target.USC_TYPEENTITY=src.USC_TYPEENTITY, 
                    target.USC_UPDATECOUNT=src.USC_UPDATECOUNT, 
                    target.USC_UPDATED=src.USC_UPDATED, 
                    target.USC_UPDATEDBY=src.USC_UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    USC_ALLOWCOMMENTS, 
                    USC_ALLOWDOCUMENTS, 
                    USC_AUTOGENERATEKEY, 
                    USC_CLASS, 
                    USC_CREATED, 
                    USC_CREATEDBY, 
                    USC_DESC, 
                    USC_ENTITY, 
                    USC_GENERATED, 
                    USC_ISTAB, 
                    USC_LASTSAVED, 
                    USC_NOTUSED, 
                    USC_ORGSECURITY, 
                    USC_PARENTSCREEN, 
                    USC_SCREENNAME, 
                    USC_STATUSENTITY, 
                    USC_TABLENAME, 
                    USC_TYPEENTITY, 
                    USC_UPDATECOUNT, 
                    USC_UPDATED, 
                    USC_UPDATEDBY, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.USC_ALLOWCOMMENTS, 
                    src.USC_ALLOWDOCUMENTS, 
                    src.USC_AUTOGENERATEKEY, 
                    src.USC_CLASS, 
                    src.USC_CREATED, 
                    src.USC_CREATEDBY, 
                    src.USC_DESC, 
                    src.USC_ENTITY, 
                    src.USC_GENERATED, 
                    src.USC_ISTAB, 
                    src.USC_LASTSAVED, 
                    src.USC_NOTUSED, 
                    src.USC_ORGSECURITY, 
                    src.USC_PARENTSCREEN, 
                    src.USC_SCREENNAME, 
                    src.USC_STATUSENTITY, 
                    src.USC_TABLENAME, 
                    src.USC_TYPEENTITY, 
                    src.USC_UPDATECOUNT, 
                    src.USC_UPDATED, 
                    src.USC_UPDATEDBY, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5UDFSCREENS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.USC_ALLOWCOMMENTS, 
            strm.USC_ALLOWDOCUMENTS, 
            strm.USC_AUTOGENERATEKEY, 
            strm.USC_CLASS, 
            strm.USC_CREATED, 
            strm.USC_CREATEDBY, 
            strm.USC_DESC, 
            strm.USC_ENTITY, 
            strm.USC_GENERATED, 
            strm.USC_ISTAB, 
            strm.USC_LASTSAVED, 
            strm.USC_NOTUSED, 
            strm.USC_ORGSECURITY, 
            strm.USC_PARENTSCREEN, 
            strm.USC_SCREENNAME, 
            strm.USC_STATUSENTITY, 
            strm.USC_TABLENAME, 
            strm.USC_TYPEENTITY, 
            strm.USC_UPDATECOUNT, 
            strm.USC_UPDATED, 
            strm.USC_UPDATEDBY, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.USC_SCREENNAME ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5UDFSCREENS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.USC_SCREENNAME=src.USC_SCREENNAME) OR (target.USC_SCREENNAME IS NULL AND src.USC_SCREENNAME IS NULL)) 
                when matched then update set
                    target.USC_ALLOWCOMMENTS=src.USC_ALLOWCOMMENTS, 
                    target.USC_ALLOWDOCUMENTS=src.USC_ALLOWDOCUMENTS, 
                    target.USC_AUTOGENERATEKEY=src.USC_AUTOGENERATEKEY, 
                    target.USC_CLASS=src.USC_CLASS, 
                    target.USC_CREATED=src.USC_CREATED, 
                    target.USC_CREATEDBY=src.USC_CREATEDBY, 
                    target.USC_DESC=src.USC_DESC, 
                    target.USC_ENTITY=src.USC_ENTITY, 
                    target.USC_GENERATED=src.USC_GENERATED, 
                    target.USC_ISTAB=src.USC_ISTAB, 
                    target.USC_LASTSAVED=src.USC_LASTSAVED, 
                    target.USC_NOTUSED=src.USC_NOTUSED, 
                    target.USC_ORGSECURITY=src.USC_ORGSECURITY, 
                    target.USC_PARENTSCREEN=src.USC_PARENTSCREEN, 
                    target.USC_SCREENNAME=src.USC_SCREENNAME, 
                    target.USC_STATUSENTITY=src.USC_STATUSENTITY, 
                    target.USC_TABLENAME=src.USC_TABLENAME, 
                    target.USC_TYPEENTITY=src.USC_TYPEENTITY, 
                    target.USC_UPDATECOUNT=src.USC_UPDATECOUNT, 
                    target.USC_UPDATED=src.USC_UPDATED, 
                    target.USC_UPDATEDBY=src.USC_UPDATEDBY, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( USC_ALLOWCOMMENTS, USC_ALLOWDOCUMENTS, USC_AUTOGENERATEKEY, USC_CLASS, USC_CREATED, USC_CREATEDBY, USC_DESC, USC_ENTITY, USC_GENERATED, USC_ISTAB, USC_LASTSAVED, USC_NOTUSED, USC_ORGSECURITY, USC_PARENTSCREEN, USC_SCREENNAME, USC_STATUSENTITY, USC_TABLENAME, USC_TYPEENTITY, USC_UPDATECOUNT, USC_UPDATED, USC_UPDATEDBY, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.USC_ALLOWCOMMENTS, 
                    src.USC_ALLOWDOCUMENTS, 
                    src.USC_AUTOGENERATEKEY, 
                    src.USC_CLASS, 
                    src.USC_CREATED, 
                    src.USC_CREATEDBY, 
                    src.USC_DESC, 
                    src.USC_ENTITY, 
                    src.USC_GENERATED, 
                    src.USC_ISTAB, 
                    src.USC_LASTSAVED, 
                    src.USC_NOTUSED, 
                    src.USC_ORGSECURITY, 
                    src.USC_PARENTSCREEN, 
                    src.USC_SCREENNAME, 
                    src.USC_STATUSENTITY, 
                    src.USC_TABLENAME, 
                    src.USC_TYPEENTITY, 
                    src.USC_UPDATECOUNT, 
                    src.USC_UPDATED, 
                    src.USC_UPDATEDBY, 
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