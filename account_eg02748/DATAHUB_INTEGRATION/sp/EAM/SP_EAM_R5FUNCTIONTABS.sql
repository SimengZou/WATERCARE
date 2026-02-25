create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5FUNCTIONTABS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5FUNCTIONTABS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5FUNCTIONTABS as target using (SELECT * FROM (SELECT 
            strm.FTB_ALTERSAVE, 
            strm.FTB_DELETE, 
            strm.FTB_DESIGNPOPUP, 
            strm.FTB_DISPLAYFT, 
            strm.FTB_EWSBTN, 
            strm.FTB_FUNCTION, 
            strm.FTB_INSERT, 
            strm.FTB_INTERFACECODE, 
            strm.FTB_LASTSAVED, 
            strm.FTB_MEKEY, 
            strm.FTB_NODESIGN, 
            strm.FTB_PRODUCT, 
            strm.FTB_RENTITY, 
            strm.FTB_SELECT, 
            strm.FTB_SEQ, 
            strm.FTB_SQLEXIST, 
            strm.FTB_SYSREQUIRED, 
            strm.FTB_TAB, 
            strm.FTB_UPDATE, 
            strm.FTB_UPDATECOUNT, 
            strm.FTB_VISIBLE, 
            strm.FTB_XTYPE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.FTB_FUNCTION,
            strm.FTB_TAB ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FUNCTIONTABS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.FTB_FUNCTION=src.FTB_FUNCTION) OR (target.FTB_FUNCTION IS NULL AND src.FTB_FUNCTION IS NULL)) AND
            ((target.FTB_TAB=src.FTB_TAB) OR (target.FTB_TAB IS NULL AND src.FTB_TAB IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.FTB_ALTERSAVE=src.FTB_ALTERSAVE, 
                    target.FTB_DELETE=src.FTB_DELETE, 
                    target.FTB_DESIGNPOPUP=src.FTB_DESIGNPOPUP, 
                    target.FTB_DISPLAYFT=src.FTB_DISPLAYFT, 
                    target.FTB_EWSBTN=src.FTB_EWSBTN, 
                    target.FTB_FUNCTION=src.FTB_FUNCTION, 
                    target.FTB_INSERT=src.FTB_INSERT, 
                    target.FTB_INTERFACECODE=src.FTB_INTERFACECODE, 
                    target.FTB_LASTSAVED=src.FTB_LASTSAVED, 
                    target.FTB_MEKEY=src.FTB_MEKEY, 
                    target.FTB_NODESIGN=src.FTB_NODESIGN, 
                    target.FTB_PRODUCT=src.FTB_PRODUCT, 
                    target.FTB_RENTITY=src.FTB_RENTITY, 
                    target.FTB_SELECT=src.FTB_SELECT, 
                    target.FTB_SEQ=src.FTB_SEQ, 
                    target.FTB_SQLEXIST=src.FTB_SQLEXIST, 
                    target.FTB_SYSREQUIRED=src.FTB_SYSREQUIRED, 
                    target.FTB_TAB=src.FTB_TAB, 
                    target.FTB_UPDATE=src.FTB_UPDATE, 
                    target.FTB_UPDATECOUNT=src.FTB_UPDATECOUNT, 
                    target.FTB_VISIBLE=src.FTB_VISIBLE, 
                    target.FTB_XTYPE=src.FTB_XTYPE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    FTB_ALTERSAVE, 
                    FTB_DELETE, 
                    FTB_DESIGNPOPUP, 
                    FTB_DISPLAYFT, 
                    FTB_EWSBTN, 
                    FTB_FUNCTION, 
                    FTB_INSERT, 
                    FTB_INTERFACECODE, 
                    FTB_LASTSAVED, 
                    FTB_MEKEY, 
                    FTB_NODESIGN, 
                    FTB_PRODUCT, 
                    FTB_RENTITY, 
                    FTB_SELECT, 
                    FTB_SEQ, 
                    FTB_SQLEXIST, 
                    FTB_SYSREQUIRED, 
                    FTB_TAB, 
                    FTB_UPDATE, 
                    FTB_UPDATECOUNT, 
                    FTB_VISIBLE, 
                    FTB_XTYPE, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.FTB_ALTERSAVE, 
                    src.FTB_DELETE, 
                    src.FTB_DESIGNPOPUP, 
                    src.FTB_DISPLAYFT, 
                    src.FTB_EWSBTN, 
                    src.FTB_FUNCTION, 
                    src.FTB_INSERT, 
                    src.FTB_INTERFACECODE, 
                    src.FTB_LASTSAVED, 
                    src.FTB_MEKEY, 
                    src.FTB_NODESIGN, 
                    src.FTB_PRODUCT, 
                    src.FTB_RENTITY, 
                    src.FTB_SELECT, 
                    src.FTB_SEQ, 
                    src.FTB_SQLEXIST, 
                    src.FTB_SYSREQUIRED, 
                    src.FTB_TAB, 
                    src.FTB_UPDATE, 
                    src.FTB_UPDATECOUNT, 
                    src.FTB_VISIBLE, 
                    src.FTB_XTYPE, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5FUNCTIONTABS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.FTB_ALTERSAVE, 
            strm.FTB_DELETE, 
            strm.FTB_DESIGNPOPUP, 
            strm.FTB_DISPLAYFT, 
            strm.FTB_EWSBTN, 
            strm.FTB_FUNCTION, 
            strm.FTB_INSERT, 
            strm.FTB_INTERFACECODE, 
            strm.FTB_LASTSAVED, 
            strm.FTB_MEKEY, 
            strm.FTB_NODESIGN, 
            strm.FTB_PRODUCT, 
            strm.FTB_RENTITY, 
            strm.FTB_SELECT, 
            strm.FTB_SEQ, 
            strm.FTB_SQLEXIST, 
            strm.FTB_SYSREQUIRED, 
            strm.FTB_TAB, 
            strm.FTB_UPDATE, 
            strm.FTB_UPDATECOUNT, 
            strm.FTB_VISIBLE, 
            strm.FTB_XTYPE, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.FTB_FUNCTION,
            strm.FTB_TAB ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5FUNCTIONTABS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.FTB_FUNCTION=src.FTB_FUNCTION) OR (target.FTB_FUNCTION IS NULL AND src.FTB_FUNCTION IS NULL)) AND
            ((target.FTB_TAB=src.FTB_TAB) OR (target.FTB_TAB IS NULL AND src.FTB_TAB IS NULL)) 
                when matched then update set
                    target.FTB_ALTERSAVE=src.FTB_ALTERSAVE, 
                    target.FTB_DELETE=src.FTB_DELETE, 
                    target.FTB_DESIGNPOPUP=src.FTB_DESIGNPOPUP, 
                    target.FTB_DISPLAYFT=src.FTB_DISPLAYFT, 
                    target.FTB_EWSBTN=src.FTB_EWSBTN, 
                    target.FTB_FUNCTION=src.FTB_FUNCTION, 
                    target.FTB_INSERT=src.FTB_INSERT, 
                    target.FTB_INTERFACECODE=src.FTB_INTERFACECODE, 
                    target.FTB_LASTSAVED=src.FTB_LASTSAVED, 
                    target.FTB_MEKEY=src.FTB_MEKEY, 
                    target.FTB_NODESIGN=src.FTB_NODESIGN, 
                    target.FTB_PRODUCT=src.FTB_PRODUCT, 
                    target.FTB_RENTITY=src.FTB_RENTITY, 
                    target.FTB_SELECT=src.FTB_SELECT, 
                    target.FTB_SEQ=src.FTB_SEQ, 
                    target.FTB_SQLEXIST=src.FTB_SQLEXIST, 
                    target.FTB_SYSREQUIRED=src.FTB_SYSREQUIRED, 
                    target.FTB_TAB=src.FTB_TAB, 
                    target.FTB_UPDATE=src.FTB_UPDATE, 
                    target.FTB_UPDATECOUNT=src.FTB_UPDATECOUNT, 
                    target.FTB_VISIBLE=src.FTB_VISIBLE, 
                    target.FTB_XTYPE=src.FTB_XTYPE, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( FTB_ALTERSAVE, FTB_DELETE, FTB_DESIGNPOPUP, FTB_DISPLAYFT, FTB_EWSBTN, FTB_FUNCTION, FTB_INSERT, FTB_INTERFACECODE, FTB_LASTSAVED, FTB_MEKEY, FTB_NODESIGN, FTB_PRODUCT, FTB_RENTITY, FTB_SELECT, FTB_SEQ, FTB_SQLEXIST, FTB_SYSREQUIRED, FTB_TAB, FTB_UPDATE, FTB_UPDATECOUNT, FTB_VISIBLE, FTB_XTYPE, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.FTB_ALTERSAVE, 
                    src.FTB_DELETE, 
                    src.FTB_DESIGNPOPUP, 
                    src.FTB_DISPLAYFT, 
                    src.FTB_EWSBTN, 
                    src.FTB_FUNCTION, 
                    src.FTB_INSERT, 
                    src.FTB_INTERFACECODE, 
                    src.FTB_LASTSAVED, 
                    src.FTB_MEKEY, 
                    src.FTB_NODESIGN, 
                    src.FTB_PRODUCT, 
                    src.FTB_RENTITY, 
                    src.FTB_SELECT, 
                    src.FTB_SEQ, 
                    src.FTB_SQLEXIST, 
                    src.FTB_SYSREQUIRED, 
                    src.FTB_TAB, 
                    src.FTB_UPDATE, 
                    src.FTB_UPDATECOUNT, 
                    src.FTB_VISIBLE, 
                    src.FTB_XTYPE, 
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