create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5CLOSINGCODEHIERARCHY()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5CLOSINGCODEHIERARCHY_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5CLOSINGCODEHIERARCHY as target using (SELECT * FROM (SELECT 
            strm.CCH_CHILDCLOSINGCODE, 
            strm.CCH_CHILDCLOSINGCODETYPE, 
            strm.CCH_LASTSAVED, 
            strm.CCH_OBJECT, 
            strm.CCH_OBJECT_ORG, 
            strm.CCH_PARENTCLOSINGCODE, 
            strm.CCH_PARENTCLOSINGCODETYPE, 
            strm.CCH_REPLDEPT, 
            strm.CCH_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CCH_PARENTCLOSINGCODE,
            strm.CCH_PARENTCLOSINGCODETYPE,
            strm.CCH_CHILDCLOSINGCODE,
            strm.CCH_CHILDCLOSINGCODETYPE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CLOSINGCODEHIERARCHY as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CCH_PARENTCLOSINGCODE=src.CCH_PARENTCLOSINGCODE) OR (target.CCH_PARENTCLOSINGCODE IS NULL AND src.CCH_PARENTCLOSINGCODE IS NULL)) AND
            ((target.CCH_PARENTCLOSINGCODETYPE=src.CCH_PARENTCLOSINGCODETYPE) OR (target.CCH_PARENTCLOSINGCODETYPE IS NULL AND src.CCH_PARENTCLOSINGCODETYPE IS NULL)) AND
            ((target.CCH_CHILDCLOSINGCODE=src.CCH_CHILDCLOSINGCODE) OR (target.CCH_CHILDCLOSINGCODE IS NULL AND src.CCH_CHILDCLOSINGCODE IS NULL)) AND
            ((target.CCH_CHILDCLOSINGCODETYPE=src.CCH_CHILDCLOSINGCODETYPE) OR (target.CCH_CHILDCLOSINGCODETYPE IS NULL AND src.CCH_CHILDCLOSINGCODETYPE IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CCH_CHILDCLOSINGCODE=src.CCH_CHILDCLOSINGCODE, 
                    target.CCH_CHILDCLOSINGCODETYPE=src.CCH_CHILDCLOSINGCODETYPE, 
                    target.CCH_LASTSAVED=src.CCH_LASTSAVED, 
                    target.CCH_OBJECT=src.CCH_OBJECT, 
                    target.CCH_OBJECT_ORG=src.CCH_OBJECT_ORG, 
                    target.CCH_PARENTCLOSINGCODE=src.CCH_PARENTCLOSINGCODE, 
                    target.CCH_PARENTCLOSINGCODETYPE=src.CCH_PARENTCLOSINGCODETYPE, 
                    target.CCH_REPLDEPT=src.CCH_REPLDEPT, 
                    target.CCH_UPDATECOUNT=src.CCH_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CCH_CHILDCLOSINGCODE, 
                    CCH_CHILDCLOSINGCODETYPE, 
                    CCH_LASTSAVED, 
                    CCH_OBJECT, 
                    CCH_OBJECT_ORG, 
                    CCH_PARENTCLOSINGCODE, 
                    CCH_PARENTCLOSINGCODETYPE, 
                    CCH_REPLDEPT, 
                    CCH_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CCH_CHILDCLOSINGCODE, 
                    src.CCH_CHILDCLOSINGCODETYPE, 
                    src.CCH_LASTSAVED, 
                    src.CCH_OBJECT, 
                    src.CCH_OBJECT_ORG, 
                    src.CCH_PARENTCLOSINGCODE, 
                    src.CCH_PARENTCLOSINGCODETYPE, 
                    src.CCH_REPLDEPT, 
                    src.CCH_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5CLOSINGCODEHIERARCHY_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CCH_CHILDCLOSINGCODE, 
            strm.CCH_CHILDCLOSINGCODETYPE, 
            strm.CCH_LASTSAVED, 
            strm.CCH_OBJECT, 
            strm.CCH_OBJECT_ORG, 
            strm.CCH_PARENTCLOSINGCODE, 
            strm.CCH_PARENTCLOSINGCODETYPE, 
            strm.CCH_REPLDEPT, 
            strm.CCH_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CCH_PARENTCLOSINGCODE,
            strm.CCH_PARENTCLOSINGCODETYPE,
            strm.CCH_CHILDCLOSINGCODE,
            strm.CCH_CHILDCLOSINGCODETYPE ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CLOSINGCODEHIERARCHY as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CCH_PARENTCLOSINGCODE=src.CCH_PARENTCLOSINGCODE) OR (target.CCH_PARENTCLOSINGCODE IS NULL AND src.CCH_PARENTCLOSINGCODE IS NULL)) AND
            ((target.CCH_PARENTCLOSINGCODETYPE=src.CCH_PARENTCLOSINGCODETYPE) OR (target.CCH_PARENTCLOSINGCODETYPE IS NULL AND src.CCH_PARENTCLOSINGCODETYPE IS NULL)) AND
            ((target.CCH_CHILDCLOSINGCODE=src.CCH_CHILDCLOSINGCODE) OR (target.CCH_CHILDCLOSINGCODE IS NULL AND src.CCH_CHILDCLOSINGCODE IS NULL)) AND
            ((target.CCH_CHILDCLOSINGCODETYPE=src.CCH_CHILDCLOSINGCODETYPE) OR (target.CCH_CHILDCLOSINGCODETYPE IS NULL AND src.CCH_CHILDCLOSINGCODETYPE IS NULL)) 
                when matched then update set
                    target.CCH_CHILDCLOSINGCODE=src.CCH_CHILDCLOSINGCODE, 
                    target.CCH_CHILDCLOSINGCODETYPE=src.CCH_CHILDCLOSINGCODETYPE, 
                    target.CCH_LASTSAVED=src.CCH_LASTSAVED, 
                    target.CCH_OBJECT=src.CCH_OBJECT, 
                    target.CCH_OBJECT_ORG=src.CCH_OBJECT_ORG, 
                    target.CCH_PARENTCLOSINGCODE=src.CCH_PARENTCLOSINGCODE, 
                    target.CCH_PARENTCLOSINGCODETYPE=src.CCH_PARENTCLOSINGCODETYPE, 
                    target.CCH_REPLDEPT=src.CCH_REPLDEPT, 
                    target.CCH_UPDATECOUNT=src.CCH_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CCH_CHILDCLOSINGCODE, CCH_CHILDCLOSINGCODETYPE, CCH_LASTSAVED, CCH_OBJECT, CCH_OBJECT_ORG, CCH_PARENTCLOSINGCODE, CCH_PARENTCLOSINGCODETYPE, CCH_REPLDEPT, CCH_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CCH_CHILDCLOSINGCODE, 
                    src.CCH_CHILDCLOSINGCODETYPE, 
                    src.CCH_LASTSAVED, 
                    src.CCH_OBJECT, 
                    src.CCH_OBJECT_ORG, 
                    src.CCH_PARENTCLOSINGCODE, 
                    src.CCH_PARENTCLOSINGCODETYPE, 
                    src.CCH_REPLDEPT, 
                    src.CCH_UPDATECOUNT, 
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