create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5RELIABILITYRANKINGLEVELS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5RELIABILITYRANKINGLEVELS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5RELIABILITYRANKINGLEVELS as target using (SELECT * FROM (SELECT 
            strm.RRL_ALLOWOPERATORCHECKLIST, 
            strm.RRL_ASPECT, 
            strm.RRL_CALCULATED, 
            strm.RRL_CHECKLISTTYPE, 
            strm.RRL_CODE, 
            strm.RRL_DESC, 
            strm.RRL_FORMULA, 
            strm.RRL_INTEGER, 
            strm.RRL_LASTSAVED, 
            strm.RRL_MAX, 
            strm.RRL_MIN, 
            strm.RRL_NUMERIC, 
            strm.RRL_ORG, 
            strm.RRL_PARENT, 
            strm.RRL_PK, 
            strm.RRL_QUERYCODE, 
            strm.RRL_QUESTION, 
            strm.RRL_QUESTIONLEVEL, 
            strm.RRL_RELIABILITYRANKING, 
            strm.RRL_REVISION, 
            strm.RRL_SEQ, 
            strm.RRL_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RRL_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5RELIABILITYRANKINGLEVELS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RRL_PK=src.RRL_PK) OR (target.RRL_PK IS NULL AND src.RRL_PK IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.RRL_ALLOWOPERATORCHECKLIST=src.RRL_ALLOWOPERATORCHECKLIST, 
                    target.RRL_ASPECT=src.RRL_ASPECT, 
                    target.RRL_CALCULATED=src.RRL_CALCULATED, 
                    target.RRL_CHECKLISTTYPE=src.RRL_CHECKLISTTYPE, 
                    target.RRL_CODE=src.RRL_CODE, 
                    target.RRL_DESC=src.RRL_DESC, 
                    target.RRL_FORMULA=src.RRL_FORMULA, 
                    target.RRL_INTEGER=src.RRL_INTEGER, 
                    target.RRL_LASTSAVED=src.RRL_LASTSAVED, 
                    target.RRL_MAX=src.RRL_MAX, 
                    target.RRL_MIN=src.RRL_MIN, 
                    target.RRL_NUMERIC=src.RRL_NUMERIC, 
                    target.RRL_ORG=src.RRL_ORG, 
                    target.RRL_PARENT=src.RRL_PARENT, 
                    target.RRL_PK=src.RRL_PK, 
                    target.RRL_QUERYCODE=src.RRL_QUERYCODE, 
                    target.RRL_QUESTION=src.RRL_QUESTION, 
                    target.RRL_QUESTIONLEVEL=src.RRL_QUESTIONLEVEL, 
                    target.RRL_RELIABILITYRANKING=src.RRL_RELIABILITYRANKING, 
                    target.RRL_REVISION=src.RRL_REVISION, 
                    target.RRL_SEQ=src.RRL_SEQ, 
                    target.RRL_UPDATECOUNT=src.RRL_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    RRL_ALLOWOPERATORCHECKLIST, 
                    RRL_ASPECT, 
                    RRL_CALCULATED, 
                    RRL_CHECKLISTTYPE, 
                    RRL_CODE, 
                    RRL_DESC, 
                    RRL_FORMULA, 
                    RRL_INTEGER, 
                    RRL_LASTSAVED, 
                    RRL_MAX, 
                    RRL_MIN, 
                    RRL_NUMERIC, 
                    RRL_ORG, 
                    RRL_PARENT, 
                    RRL_PK, 
                    RRL_QUERYCODE, 
                    RRL_QUESTION, 
                    RRL_QUESTIONLEVEL, 
                    RRL_RELIABILITYRANKING, 
                    RRL_REVISION, 
                    RRL_SEQ, 
                    RRL_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.RRL_ALLOWOPERATORCHECKLIST, 
                    src.RRL_ASPECT, 
                    src.RRL_CALCULATED, 
                    src.RRL_CHECKLISTTYPE, 
                    src.RRL_CODE, 
                    src.RRL_DESC, 
                    src.RRL_FORMULA, 
                    src.RRL_INTEGER, 
                    src.RRL_LASTSAVED, 
                    src.RRL_MAX, 
                    src.RRL_MIN, 
                    src.RRL_NUMERIC, 
                    src.RRL_ORG, 
                    src.RRL_PARENT, 
                    src.RRL_PK, 
                    src.RRL_QUERYCODE, 
                    src.RRL_QUESTION, 
                    src.RRL_QUESTIONLEVEL, 
                    src.RRL_RELIABILITYRANKING, 
                    src.RRL_REVISION, 
                    src.RRL_SEQ, 
                    src.RRL_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5RELIABILITYRANKINGLEVELS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.RRL_ALLOWOPERATORCHECKLIST, 
            strm.RRL_ASPECT, 
            strm.RRL_CALCULATED, 
            strm.RRL_CHECKLISTTYPE, 
            strm.RRL_CODE, 
            strm.RRL_DESC, 
            strm.RRL_FORMULA, 
            strm.RRL_INTEGER, 
            strm.RRL_LASTSAVED, 
            strm.RRL_MAX, 
            strm.RRL_MIN, 
            strm.RRL_NUMERIC, 
            strm.RRL_ORG, 
            strm.RRL_PARENT, 
            strm.RRL_PK, 
            strm.RRL_QUERYCODE, 
            strm.RRL_QUESTION, 
            strm.RRL_QUESTIONLEVEL, 
            strm.RRL_RELIABILITYRANKING, 
            strm.RRL_REVISION, 
            strm.RRL_SEQ, 
            strm.RRL_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RRL_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5RELIABILITYRANKINGLEVELS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RRL_PK=src.RRL_PK) OR (target.RRL_PK IS NULL AND src.RRL_PK IS NULL)) 
                when matched then update set
                    target.RRL_ALLOWOPERATORCHECKLIST=src.RRL_ALLOWOPERATORCHECKLIST, 
                    target.RRL_ASPECT=src.RRL_ASPECT, 
                    target.RRL_CALCULATED=src.RRL_CALCULATED, 
                    target.RRL_CHECKLISTTYPE=src.RRL_CHECKLISTTYPE, 
                    target.RRL_CODE=src.RRL_CODE, 
                    target.RRL_DESC=src.RRL_DESC, 
                    target.RRL_FORMULA=src.RRL_FORMULA, 
                    target.RRL_INTEGER=src.RRL_INTEGER, 
                    target.RRL_LASTSAVED=src.RRL_LASTSAVED, 
                    target.RRL_MAX=src.RRL_MAX, 
                    target.RRL_MIN=src.RRL_MIN, 
                    target.RRL_NUMERIC=src.RRL_NUMERIC, 
                    target.RRL_ORG=src.RRL_ORG, 
                    target.RRL_PARENT=src.RRL_PARENT, 
                    target.RRL_PK=src.RRL_PK, 
                    target.RRL_QUERYCODE=src.RRL_QUERYCODE, 
                    target.RRL_QUESTION=src.RRL_QUESTION, 
                    target.RRL_QUESTIONLEVEL=src.RRL_QUESTIONLEVEL, 
                    target.RRL_RELIABILITYRANKING=src.RRL_RELIABILITYRANKING, 
                    target.RRL_REVISION=src.RRL_REVISION, 
                    target.RRL_SEQ=src.RRL_SEQ, 
                    target.RRL_UPDATECOUNT=src.RRL_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( RRL_ALLOWOPERATORCHECKLIST, RRL_ASPECT, RRL_CALCULATED, RRL_CHECKLISTTYPE, RRL_CODE, RRL_DESC, RRL_FORMULA, RRL_INTEGER, RRL_LASTSAVED, RRL_MAX, RRL_MIN, RRL_NUMERIC, RRL_ORG, RRL_PARENT, RRL_PK, RRL_QUERYCODE, RRL_QUESTION, RRL_QUESTIONLEVEL, RRL_RELIABILITYRANKING, RRL_REVISION, RRL_SEQ, RRL_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.RRL_ALLOWOPERATORCHECKLIST, 
                    src.RRL_ASPECT, 
                    src.RRL_CALCULATED, 
                    src.RRL_CHECKLISTTYPE, 
                    src.RRL_CODE, 
                    src.RRL_DESC, 
                    src.RRL_FORMULA, 
                    src.RRL_INTEGER, 
                    src.RRL_LASTSAVED, 
                    src.RRL_MAX, 
                    src.RRL_MIN, 
                    src.RRL_NUMERIC, 
                    src.RRL_ORG, 
                    src.RRL_PARENT, 
                    src.RRL_PK, 
                    src.RRL_QUERYCODE, 
                    src.RRL_QUESTION, 
                    src.RRL_QUESTIONLEVEL, 
                    src.RRL_RELIABILITYRANKING, 
                    src.RRL_REVISION, 
                    src.RRL_SEQ, 
                    src.RRL_UPDATECOUNT, 
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