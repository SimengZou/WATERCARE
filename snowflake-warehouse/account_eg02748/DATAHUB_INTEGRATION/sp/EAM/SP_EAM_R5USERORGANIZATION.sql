create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5USERORGANIZATION()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5USERORGANIZATION_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5USERORGANIZATION as target using (SELECT * FROM (SELECT 
            strm.UOG_COMMON, 
            strm.UOG_CREATED, 
            strm.UOG_DEFAULT, 
            strm.UOG_GROUP, 
            strm.UOG_INVAPPVLIMIT, 
            strm.UOG_INVAPPVLIMITNONPO, 
            strm.UOG_LASTSAVED, 
            strm.UOG_ORG, 
            strm.UOG_PICAPPVLIMIT, 
            strm.UOG_PORDAPPVLIMIT, 
            strm.UOG_PORDAUTHAPPVLIMIT, 
            strm.UOG_REQAPPVLIMIT, 
            strm.UOG_REQAUTHAPPVLIMIT, 
            strm.UOG_ROLE, 
            strm.UOG_UPDATECOUNT, 
            strm.UOG_UPDATED, 
            strm.UOG_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.UOG_USER,
            strm.UOG_ORG,
            strm.UOG_ROLE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USERORGANIZATION as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.UOG_USER=src.UOG_USER) OR (target.UOG_USER IS NULL AND src.UOG_USER IS NULL)) AND
            ((target.UOG_ORG=src.UOG_ORG) OR (target.UOG_ORG IS NULL AND src.UOG_ORG IS NULL)) AND
            ((target.UOG_ROLE=src.UOG_ROLE) OR (target.UOG_ROLE IS NULL AND src.UOG_ROLE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.UOG_COMMON=src.UOG_COMMON, 
                    target.UOG_CREATED=src.UOG_CREATED, 
                    target.UOG_DEFAULT=src.UOG_DEFAULT, 
                    target.UOG_GROUP=src.UOG_GROUP, 
                    target.UOG_INVAPPVLIMIT=src.UOG_INVAPPVLIMIT, 
                    target.UOG_INVAPPVLIMITNONPO=src.UOG_INVAPPVLIMITNONPO, 
                    target.UOG_LASTSAVED=src.UOG_LASTSAVED, 
                    target.UOG_ORG=src.UOG_ORG, 
                    target.UOG_PICAPPVLIMIT=src.UOG_PICAPPVLIMIT, 
                    target.UOG_PORDAPPVLIMIT=src.UOG_PORDAPPVLIMIT, 
                    target.UOG_PORDAUTHAPPVLIMIT=src.UOG_PORDAUTHAPPVLIMIT, 
                    target.UOG_REQAPPVLIMIT=src.UOG_REQAPPVLIMIT, 
                    target.UOG_REQAUTHAPPVLIMIT=src.UOG_REQAUTHAPPVLIMIT, 
                    target.UOG_ROLE=src.UOG_ROLE, 
                    target.UOG_UPDATECOUNT=src.UOG_UPDATECOUNT, 
                    target.UOG_UPDATED=src.UOG_UPDATED, 
                    target.UOG_USER=src.UOG_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    UOG_COMMON, 
                    UOG_CREATED, 
                    UOG_DEFAULT, 
                    UOG_GROUP, 
                    UOG_INVAPPVLIMIT, 
                    UOG_INVAPPVLIMITNONPO, 
                    UOG_LASTSAVED, 
                    UOG_ORG, 
                    UOG_PICAPPVLIMIT, 
                    UOG_PORDAPPVLIMIT, 
                    UOG_PORDAUTHAPPVLIMIT, 
                    UOG_REQAPPVLIMIT, 
                    UOG_REQAUTHAPPVLIMIT, 
                    UOG_ROLE, 
                    UOG_UPDATECOUNT, 
                    UOG_UPDATED, 
                    UOG_USER, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.UOG_COMMON, 
                    src.UOG_CREATED, 
                    src.UOG_DEFAULT, 
                    src.UOG_GROUP, 
                    src.UOG_INVAPPVLIMIT, 
                    src.UOG_INVAPPVLIMITNONPO, 
                    src.UOG_LASTSAVED, 
                    src.UOG_ORG, 
                    src.UOG_PICAPPVLIMIT, 
                    src.UOG_PORDAPPVLIMIT, 
                    src.UOG_PORDAUTHAPPVLIMIT, 
                    src.UOG_REQAPPVLIMIT, 
                    src.UOG_REQAUTHAPPVLIMIT, 
                    src.UOG_ROLE, 
                    src.UOG_UPDATECOUNT, 
                    src.UOG_UPDATED, 
                    src.UOG_USER, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5USERORGANIZATION_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.UOG_COMMON, 
            strm.UOG_CREATED, 
            strm.UOG_DEFAULT, 
            strm.UOG_GROUP, 
            strm.UOG_INVAPPVLIMIT, 
            strm.UOG_INVAPPVLIMITNONPO, 
            strm.UOG_LASTSAVED, 
            strm.UOG_ORG, 
            strm.UOG_PICAPPVLIMIT, 
            strm.UOG_PORDAPPVLIMIT, 
            strm.UOG_PORDAUTHAPPVLIMIT, 
            strm.UOG_REQAPPVLIMIT, 
            strm.UOG_REQAUTHAPPVLIMIT, 
            strm.UOG_ROLE, 
            strm.UOG_UPDATECOUNT, 
            strm.UOG_UPDATED, 
            strm.UOG_USER, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.UOG_USER,
            strm.UOG_ORG,
            strm.UOG_ROLE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USERORGANIZATION as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.UOG_USER=src.UOG_USER) OR (target.UOG_USER IS NULL AND src.UOG_USER IS NULL)) AND
            ((target.UOG_ORG=src.UOG_ORG) OR (target.UOG_ORG IS NULL AND src.UOG_ORG IS NULL)) AND
            ((target.UOG_ROLE=src.UOG_ROLE) OR (target.UOG_ROLE IS NULL AND src.UOG_ROLE IS NULL)) 
                when matched then update set
                    target.UOG_COMMON=src.UOG_COMMON, 
                    target.UOG_CREATED=src.UOG_CREATED, 
                    target.UOG_DEFAULT=src.UOG_DEFAULT, 
                    target.UOG_GROUP=src.UOG_GROUP, 
                    target.UOG_INVAPPVLIMIT=src.UOG_INVAPPVLIMIT, 
                    target.UOG_INVAPPVLIMITNONPO=src.UOG_INVAPPVLIMITNONPO, 
                    target.UOG_LASTSAVED=src.UOG_LASTSAVED, 
                    target.UOG_ORG=src.UOG_ORG, 
                    target.UOG_PICAPPVLIMIT=src.UOG_PICAPPVLIMIT, 
                    target.UOG_PORDAPPVLIMIT=src.UOG_PORDAPPVLIMIT, 
                    target.UOG_PORDAUTHAPPVLIMIT=src.UOG_PORDAUTHAPPVLIMIT, 
                    target.UOG_REQAPPVLIMIT=src.UOG_REQAPPVLIMIT, 
                    target.UOG_REQAUTHAPPVLIMIT=src.UOG_REQAUTHAPPVLIMIT, 
                    target.UOG_ROLE=src.UOG_ROLE, 
                    target.UOG_UPDATECOUNT=src.UOG_UPDATECOUNT, 
                    target.UOG_UPDATED=src.UOG_UPDATED, 
                    target.UOG_USER=src.UOG_USER, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( UOG_COMMON, UOG_CREATED, UOG_DEFAULT, UOG_GROUP, UOG_INVAPPVLIMIT, UOG_INVAPPVLIMITNONPO, UOG_LASTSAVED, UOG_ORG, UOG_PICAPPVLIMIT, UOG_PORDAPPVLIMIT, UOG_PORDAUTHAPPVLIMIT, UOG_REQAPPVLIMIT, UOG_REQAUTHAPPVLIMIT, UOG_ROLE, UOG_UPDATECOUNT, UOG_UPDATED, UOG_USER, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.UOG_COMMON, 
                    src.UOG_CREATED, 
                    src.UOG_DEFAULT, 
                    src.UOG_GROUP, 
                    src.UOG_INVAPPVLIMIT, 
                    src.UOG_INVAPPVLIMITNONPO, 
                    src.UOG_LASTSAVED, 
                    src.UOG_ORG, 
                    src.UOG_PICAPPVLIMIT, 
                    src.UOG_PORDAPPVLIMIT, 
                    src.UOG_PORDAUTHAPPVLIMIT, 
                    src.UOG_REQAPPVLIMIT, 
                    src.UOG_REQAUTHAPPVLIMIT, 
                    src.UOG_ROLE, 
                    src.UOG_UPDATECOUNT, 
                    src.UOG_UPDATED, 
                    src.UOG_USER, 
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