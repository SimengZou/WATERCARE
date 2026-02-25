create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ADDPROPERTIES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ADDPROPERTIES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ADDPROPERTIES as target using (SELECT * FROM (SELECT 
            strm.APR_CLASS, 
            strm.APR_CLASS_ORG, 
            strm.APR_CREATED, 
            strm.APR_GROUPLABEL, 
            strm.APR_LASTSAVED, 
            strm.APR_LINE, 
            strm.APR_LIST, 
            strm.APR_LISTVALID, 
            strm.APR_NONUPDCAT, 
            strm.APR_PROPERTY, 
            strm.APR_RENTITY, 
            strm.APR_REQUIRED, 
            strm.APR_STATUS, 
            strm.APR_UOM, 
            strm.APR_UPDATECOUNT, 
            strm.APR_UPDATED, 
            strm.APR_WODISP, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APR_PROPERTY,
            strm.APR_RENTITY,
            strm.APR_CLASS,
            strm.APR_CLASS_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ADDPROPERTIES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APR_PROPERTY=src.APR_PROPERTY) OR (target.APR_PROPERTY IS NULL AND src.APR_PROPERTY IS NULL)) AND
            ((target.APR_RENTITY=src.APR_RENTITY) OR (target.APR_RENTITY IS NULL AND src.APR_RENTITY IS NULL)) AND
            ((target.APR_CLASS=src.APR_CLASS) OR (target.APR_CLASS IS NULL AND src.APR_CLASS IS NULL)) AND
            ((target.APR_CLASS_ORG=src.APR_CLASS_ORG) OR (target.APR_CLASS_ORG IS NULL AND src.APR_CLASS_ORG IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.APR_CLASS=src.APR_CLASS, 
                    target.APR_CLASS_ORG=src.APR_CLASS_ORG, 
                    target.APR_CREATED=src.APR_CREATED, 
                    target.APR_GROUPLABEL=src.APR_GROUPLABEL, 
                    target.APR_LASTSAVED=src.APR_LASTSAVED, 
                    target.APR_LINE=src.APR_LINE, 
                    target.APR_LIST=src.APR_LIST, 
                    target.APR_LISTVALID=src.APR_LISTVALID, 
                    target.APR_NONUPDCAT=src.APR_NONUPDCAT, 
                    target.APR_PROPERTY=src.APR_PROPERTY, 
                    target.APR_RENTITY=src.APR_RENTITY, 
                    target.APR_REQUIRED=src.APR_REQUIRED, 
                    target.APR_STATUS=src.APR_STATUS, 
                    target.APR_UOM=src.APR_UOM, 
                    target.APR_UPDATECOUNT=src.APR_UPDATECOUNT, 
                    target.APR_UPDATED=src.APR_UPDATED, 
                    target.APR_WODISP=src.APR_WODISP, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    APR_CLASS, 
                    APR_CLASS_ORG, 
                    APR_CREATED, 
                    APR_GROUPLABEL, 
                    APR_LASTSAVED, 
                    APR_LINE, 
                    APR_LIST, 
                    APR_LISTVALID, 
                    APR_NONUPDCAT, 
                    APR_PROPERTY, 
                    APR_RENTITY, 
                    APR_REQUIRED, 
                    APR_STATUS, 
                    APR_UOM, 
                    APR_UPDATECOUNT, 
                    APR_UPDATED, 
                    APR_WODISP, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.APR_CLASS, 
                    src.APR_CLASS_ORG, 
                    src.APR_CREATED, 
                    src.APR_GROUPLABEL, 
                    src.APR_LASTSAVED, 
                    src.APR_LINE, 
                    src.APR_LIST, 
                    src.APR_LISTVALID, 
                    src.APR_NONUPDCAT, 
                    src.APR_PROPERTY, 
                    src.APR_RENTITY, 
                    src.APR_REQUIRED, 
                    src.APR_STATUS, 
                    src.APR_UOM, 
                    src.APR_UPDATECOUNT, 
                    src.APR_UPDATED, 
                    src.APR_WODISP, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ADDPROPERTIES_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.APR_CLASS, 
            strm.APR_CLASS_ORG, 
            strm.APR_CREATED, 
            strm.APR_GROUPLABEL, 
            strm.APR_LASTSAVED, 
            strm.APR_LINE, 
            strm.APR_LIST, 
            strm.APR_LISTVALID, 
            strm.APR_NONUPDCAT, 
            strm.APR_PROPERTY, 
            strm.APR_RENTITY, 
            strm.APR_REQUIRED, 
            strm.APR_STATUS, 
            strm.APR_UOM, 
            strm.APR_UPDATECOUNT, 
            strm.APR_UPDATED, 
            strm.APR_WODISP, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.APR_PROPERTY,
            strm.APR_RENTITY,
            strm.APR_CLASS,
            strm.APR_CLASS_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ADDPROPERTIES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.APR_PROPERTY=src.APR_PROPERTY) OR (target.APR_PROPERTY IS NULL AND src.APR_PROPERTY IS NULL)) AND
            ((target.APR_RENTITY=src.APR_RENTITY) OR (target.APR_RENTITY IS NULL AND src.APR_RENTITY IS NULL)) AND
            ((target.APR_CLASS=src.APR_CLASS) OR (target.APR_CLASS IS NULL AND src.APR_CLASS IS NULL)) AND
            ((target.APR_CLASS_ORG=src.APR_CLASS_ORG) OR (target.APR_CLASS_ORG IS NULL AND src.APR_CLASS_ORG IS NULL)) 
                when matched then update set
                    target.APR_CLASS=src.APR_CLASS, 
                    target.APR_CLASS_ORG=src.APR_CLASS_ORG, 
                    target.APR_CREATED=src.APR_CREATED, 
                    target.APR_GROUPLABEL=src.APR_GROUPLABEL, 
                    target.APR_LASTSAVED=src.APR_LASTSAVED, 
                    target.APR_LINE=src.APR_LINE, 
                    target.APR_LIST=src.APR_LIST, 
                    target.APR_LISTVALID=src.APR_LISTVALID, 
                    target.APR_NONUPDCAT=src.APR_NONUPDCAT, 
                    target.APR_PROPERTY=src.APR_PROPERTY, 
                    target.APR_RENTITY=src.APR_RENTITY, 
                    target.APR_REQUIRED=src.APR_REQUIRED, 
                    target.APR_STATUS=src.APR_STATUS, 
                    target.APR_UOM=src.APR_UOM, 
                    target.APR_UPDATECOUNT=src.APR_UPDATECOUNT, 
                    target.APR_UPDATED=src.APR_UPDATED, 
                    target.APR_WODISP=src.APR_WODISP, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( APR_CLASS, APR_CLASS_ORG, APR_CREATED, APR_GROUPLABEL, APR_LASTSAVED, APR_LINE, APR_LIST, APR_LISTVALID, APR_NONUPDCAT, APR_PROPERTY, APR_RENTITY, APR_REQUIRED, APR_STATUS, APR_UOM, APR_UPDATECOUNT, APR_UPDATED, APR_WODISP, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.APR_CLASS, 
                    src.APR_CLASS_ORG, 
                    src.APR_CREATED, 
                    src.APR_GROUPLABEL, 
                    src.APR_LASTSAVED, 
                    src.APR_LINE, 
                    src.APR_LIST, 
                    src.APR_LISTVALID, 
                    src.APR_NONUPDCAT, 
                    src.APR_PROPERTY, 
                    src.APR_RENTITY, 
                    src.APR_REQUIRED, 
                    src.APR_STATUS, 
                    src.APR_UOM, 
                    src.APR_UPDATECOUNT, 
                    src.APR_UPDATED, 
                    src.APR_WODISP, 
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