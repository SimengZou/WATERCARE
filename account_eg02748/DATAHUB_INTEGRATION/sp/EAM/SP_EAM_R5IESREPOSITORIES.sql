create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5IESREPOSITORIES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5IESREPOSITORIES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5IESREPOSITORIES as target using (SELECT * FROM (SELECT 
            strm.ENS_CODE, 
            strm.ENS_DATECREATEDCOL, 
            strm.ENS_DATELASTCRAWL, 
            strm.ENS_DATEUPDATEDCOL, 
            strm.ENS_DESC, 
            strm.ENS_INTERESTCENTER, 
            strm.ENS_ISINCREMENTAL, 
            strm.ENS_ISPOPUP, 
            strm.ENS_LASTSAVED, 
            strm.ENS_NOTUSED, 
            strm.ENS_ORGCOL, 
            strm.ENS_TAB, 
            strm.ENS_TABLENAME, 
            strm.ENS_TABLEPREFIX, 
            strm.ENS_THUMBNAIL, 
            strm.ENS_UPDATECOUNT, 
            strm.ENS_USERFUNCTION, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ENS_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5IESREPOSITORIES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ENS_CODE=src.ENS_CODE) OR (target.ENS_CODE IS NULL AND src.ENS_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ENS_CODE=src.ENS_CODE, 
                    target.ENS_DATECREATEDCOL=src.ENS_DATECREATEDCOL, 
                    target.ENS_DATELASTCRAWL=src.ENS_DATELASTCRAWL, 
                    target.ENS_DATEUPDATEDCOL=src.ENS_DATEUPDATEDCOL, 
                    target.ENS_DESC=src.ENS_DESC, 
                    target.ENS_INTERESTCENTER=src.ENS_INTERESTCENTER, 
                    target.ENS_ISINCREMENTAL=src.ENS_ISINCREMENTAL, 
                    target.ENS_ISPOPUP=src.ENS_ISPOPUP, 
                    target.ENS_LASTSAVED=src.ENS_LASTSAVED, 
                    target.ENS_NOTUSED=src.ENS_NOTUSED, 
                    target.ENS_ORGCOL=src.ENS_ORGCOL, 
                    target.ENS_TAB=src.ENS_TAB, 
                    target.ENS_TABLENAME=src.ENS_TABLENAME, 
                    target.ENS_TABLEPREFIX=src.ENS_TABLEPREFIX, 
                    target.ENS_THUMBNAIL=src.ENS_THUMBNAIL, 
                    target.ENS_UPDATECOUNT=src.ENS_UPDATECOUNT, 
                    target.ENS_USERFUNCTION=src.ENS_USERFUNCTION, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ENS_CODE, 
                    ENS_DATECREATEDCOL, 
                    ENS_DATELASTCRAWL, 
                    ENS_DATEUPDATEDCOL, 
                    ENS_DESC, 
                    ENS_INTERESTCENTER, 
                    ENS_ISINCREMENTAL, 
                    ENS_ISPOPUP, 
                    ENS_LASTSAVED, 
                    ENS_NOTUSED, 
                    ENS_ORGCOL, 
                    ENS_TAB, 
                    ENS_TABLENAME, 
                    ENS_TABLEPREFIX, 
                    ENS_THUMBNAIL, 
                    ENS_UPDATECOUNT, 
                    ENS_USERFUNCTION, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ENS_CODE, 
                    src.ENS_DATECREATEDCOL, 
                    src.ENS_DATELASTCRAWL, 
                    src.ENS_DATEUPDATEDCOL, 
                    src.ENS_DESC, 
                    src.ENS_INTERESTCENTER, 
                    src.ENS_ISINCREMENTAL, 
                    src.ENS_ISPOPUP, 
                    src.ENS_LASTSAVED, 
                    src.ENS_NOTUSED, 
                    src.ENS_ORGCOL, 
                    src.ENS_TAB, 
                    src.ENS_TABLENAME, 
                    src.ENS_TABLEPREFIX, 
                    src.ENS_THUMBNAIL, 
                    src.ENS_UPDATECOUNT, 
                    src.ENS_USERFUNCTION, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5IESREPOSITORIES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.ENS_CODE, 
            strm.ENS_DATECREATEDCOL, 
            strm.ENS_DATELASTCRAWL, 
            strm.ENS_DATEUPDATEDCOL, 
            strm.ENS_DESC, 
            strm.ENS_INTERESTCENTER, 
            strm.ENS_ISINCREMENTAL, 
            strm.ENS_ISPOPUP, 
            strm.ENS_LASTSAVED, 
            strm.ENS_NOTUSED, 
            strm.ENS_ORGCOL, 
            strm.ENS_TAB, 
            strm.ENS_TABLENAME, 
            strm.ENS_TABLEPREFIX, 
            strm.ENS_THUMBNAIL, 
            strm.ENS_UPDATECOUNT, 
            strm.ENS_USERFUNCTION, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ENS_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5IESREPOSITORIES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ENS_CODE=src.ENS_CODE) OR (target.ENS_CODE IS NULL AND src.ENS_CODE IS NULL)) 
                when matched then update set
                    target.ENS_CODE=src.ENS_CODE, 
                    target.ENS_DATECREATEDCOL=src.ENS_DATECREATEDCOL, 
                    target.ENS_DATELASTCRAWL=src.ENS_DATELASTCRAWL, 
                    target.ENS_DATEUPDATEDCOL=src.ENS_DATEUPDATEDCOL, 
                    target.ENS_DESC=src.ENS_DESC, 
                    target.ENS_INTERESTCENTER=src.ENS_INTERESTCENTER, 
                    target.ENS_ISINCREMENTAL=src.ENS_ISINCREMENTAL, 
                    target.ENS_ISPOPUP=src.ENS_ISPOPUP, 
                    target.ENS_LASTSAVED=src.ENS_LASTSAVED, 
                    target.ENS_NOTUSED=src.ENS_NOTUSED, 
                    target.ENS_ORGCOL=src.ENS_ORGCOL, 
                    target.ENS_TAB=src.ENS_TAB, 
                    target.ENS_TABLENAME=src.ENS_TABLENAME, 
                    target.ENS_TABLEPREFIX=src.ENS_TABLEPREFIX, 
                    target.ENS_THUMBNAIL=src.ENS_THUMBNAIL, 
                    target.ENS_UPDATECOUNT=src.ENS_UPDATECOUNT, 
                    target.ENS_USERFUNCTION=src.ENS_USERFUNCTION, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ENS_CODE, ENS_DATECREATEDCOL, ENS_DATELASTCRAWL, ENS_DATEUPDATEDCOL, ENS_DESC, ENS_INTERESTCENTER, ENS_ISINCREMENTAL, ENS_ISPOPUP, ENS_LASTSAVED, ENS_NOTUSED, ENS_ORGCOL, ENS_TAB, ENS_TABLENAME, ENS_TABLEPREFIX, ENS_THUMBNAIL, ENS_UPDATECOUNT, ENS_USERFUNCTION, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ENS_CODE, 
                    src.ENS_DATECREATEDCOL, 
                    src.ENS_DATELASTCRAWL, 
                    src.ENS_DATEUPDATEDCOL, 
                    src.ENS_DESC, 
                    src.ENS_INTERESTCENTER, 
                    src.ENS_ISINCREMENTAL, 
                    src.ENS_ISPOPUP, 
                    src.ENS_LASTSAVED, 
                    src.ENS_NOTUSED, 
                    src.ENS_ORGCOL, 
                    src.ENS_TAB, 
                    src.ENS_TABLENAME, 
                    src.ENS_TABLEPREFIX, 
                    src.ENS_THUMBNAIL, 
                    src.ENS_UPDATECOUNT, 
                    src.ENS_USERFUNCTION, 
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