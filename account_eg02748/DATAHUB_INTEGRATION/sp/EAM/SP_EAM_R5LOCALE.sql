create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5LOCALE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5LOCALE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5LOCALE as target using (SELECT * FROM (SELECT 
            strm.LOC_CODE, 
            strm.LOC_DATEFMT, 
            strm.LOC_DECSYM, 
            strm.LOC_DESC, 
            strm.LOC_FRACTDIGITS, 
            strm.LOC_GRPDIGITS, 
            strm.LOC_GRPSYM, 
            strm.LOC_LASTSAVED, 
            strm.LOC_MONDECSYM, 
            strm.LOC_MONFRACT, 
            strm.LOC_MONTGRPDIGITS, 
            strm.LOC_MONTGRPSEPARATOR, 
            strm.LOC_NEGSYM, 
            strm.LOC_POSSYM, 
            strm.LOC_STARTDAY, 
            strm.LOC_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.LOC_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5LOCALE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.LOC_CODE=src.LOC_CODE) OR (target.LOC_CODE IS NULL AND src.LOC_CODE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.LOC_CODE=src.LOC_CODE, 
                    target.LOC_DATEFMT=src.LOC_DATEFMT, 
                    target.LOC_DECSYM=src.LOC_DECSYM, 
                    target.LOC_DESC=src.LOC_DESC, 
                    target.LOC_FRACTDIGITS=src.LOC_FRACTDIGITS, 
                    target.LOC_GRPDIGITS=src.LOC_GRPDIGITS, 
                    target.LOC_GRPSYM=src.LOC_GRPSYM, 
                    target.LOC_LASTSAVED=src.LOC_LASTSAVED, 
                    target.LOC_MONDECSYM=src.LOC_MONDECSYM, 
                    target.LOC_MONFRACT=src.LOC_MONFRACT, 
                    target.LOC_MONTGRPDIGITS=src.LOC_MONTGRPDIGITS, 
                    target.LOC_MONTGRPSEPARATOR=src.LOC_MONTGRPSEPARATOR, 
                    target.LOC_NEGSYM=src.LOC_NEGSYM, 
                    target.LOC_POSSYM=src.LOC_POSSYM, 
                    target.LOC_STARTDAY=src.LOC_STARTDAY, 
                    target.LOC_UPDATECOUNT=src.LOC_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    LOC_CODE, 
                    LOC_DATEFMT, 
                    LOC_DECSYM, 
                    LOC_DESC, 
                    LOC_FRACTDIGITS, 
                    LOC_GRPDIGITS, 
                    LOC_GRPSYM, 
                    LOC_LASTSAVED, 
                    LOC_MONDECSYM, 
                    LOC_MONFRACT, 
                    LOC_MONTGRPDIGITS, 
                    LOC_MONTGRPSEPARATOR, 
                    LOC_NEGSYM, 
                    LOC_POSSYM, 
                    LOC_STARTDAY, 
                    LOC_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.LOC_CODE, 
                    src.LOC_DATEFMT, 
                    src.LOC_DECSYM, 
                    src.LOC_DESC, 
                    src.LOC_FRACTDIGITS, 
                    src.LOC_GRPDIGITS, 
                    src.LOC_GRPSYM, 
                    src.LOC_LASTSAVED, 
                    src.LOC_MONDECSYM, 
                    src.LOC_MONFRACT, 
                    src.LOC_MONTGRPDIGITS, 
                    src.LOC_MONTGRPSEPARATOR, 
                    src.LOC_NEGSYM, 
                    src.LOC_POSSYM, 
                    src.LOC_STARTDAY, 
                    src.LOC_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5LOCALE_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.LOC_CODE, 
            strm.LOC_DATEFMT, 
            strm.LOC_DECSYM, 
            strm.LOC_DESC, 
            strm.LOC_FRACTDIGITS, 
            strm.LOC_GRPDIGITS, 
            strm.LOC_GRPSYM, 
            strm.LOC_LASTSAVED, 
            strm.LOC_MONDECSYM, 
            strm.LOC_MONFRACT, 
            strm.LOC_MONTGRPDIGITS, 
            strm.LOC_MONTGRPSEPARATOR, 
            strm.LOC_NEGSYM, 
            strm.LOC_POSSYM, 
            strm.LOC_STARTDAY, 
            strm.LOC_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.LOC_CODE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5LOCALE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.LOC_CODE=src.LOC_CODE) OR (target.LOC_CODE IS NULL AND src.LOC_CODE IS NULL)) 
                when matched then update set
                    target.LOC_CODE=src.LOC_CODE, 
                    target.LOC_DATEFMT=src.LOC_DATEFMT, 
                    target.LOC_DECSYM=src.LOC_DECSYM, 
                    target.LOC_DESC=src.LOC_DESC, 
                    target.LOC_FRACTDIGITS=src.LOC_FRACTDIGITS, 
                    target.LOC_GRPDIGITS=src.LOC_GRPDIGITS, 
                    target.LOC_GRPSYM=src.LOC_GRPSYM, 
                    target.LOC_LASTSAVED=src.LOC_LASTSAVED, 
                    target.LOC_MONDECSYM=src.LOC_MONDECSYM, 
                    target.LOC_MONFRACT=src.LOC_MONFRACT, 
                    target.LOC_MONTGRPDIGITS=src.LOC_MONTGRPDIGITS, 
                    target.LOC_MONTGRPSEPARATOR=src.LOC_MONTGRPSEPARATOR, 
                    target.LOC_NEGSYM=src.LOC_NEGSYM, 
                    target.LOC_POSSYM=src.LOC_POSSYM, 
                    target.LOC_STARTDAY=src.LOC_STARTDAY, 
                    target.LOC_UPDATECOUNT=src.LOC_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( LOC_CODE, LOC_DATEFMT, LOC_DECSYM, LOC_DESC, LOC_FRACTDIGITS, LOC_GRPDIGITS, LOC_GRPSYM, LOC_LASTSAVED, LOC_MONDECSYM, LOC_MONFRACT, LOC_MONTGRPDIGITS, LOC_MONTGRPSEPARATOR, LOC_NEGSYM, LOC_POSSYM, LOC_STARTDAY, LOC_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.LOC_CODE, 
                    src.LOC_DATEFMT, 
                    src.LOC_DECSYM, 
                    src.LOC_DESC, 
                    src.LOC_FRACTDIGITS, 
                    src.LOC_GRPDIGITS, 
                    src.LOC_GRPSYM, 
                    src.LOC_LASTSAVED, 
                    src.LOC_MONDECSYM, 
                    src.LOC_MONFRACT, 
                    src.LOC_MONTGRPDIGITS, 
                    src.LOC_MONTGRPSEPARATOR, 
                    src.LOC_NEGSYM, 
                    src.LOC_POSSYM, 
                    src.LOC_STARTDAY, 
                    src.LOC_UPDATECOUNT, 
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