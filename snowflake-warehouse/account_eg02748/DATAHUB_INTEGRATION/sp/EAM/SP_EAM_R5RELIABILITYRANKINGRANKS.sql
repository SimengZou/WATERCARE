create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5RELIABILITYRANKINGRANKS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5RELIABILITYRANKINGRANKS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5RELIABILITYRANKINGRANKS as target using (SELECT * FROM (SELECT 
            strm.RRR_COLOR, 
            strm.RRR_CRITICALITY, 
            strm.RRR_ICON, 
            strm.RRR_ICONPATH, 
            strm.RRR_LASTSAVED, 
            strm.RRR_MAXVALUE, 
            strm.RRR_MINVALUE, 
            strm.RRR_ORG, 
            strm.RRR_PK, 
            strm.RRR_RELIABILITYRANKING, 
            strm.RRR_REVISION, 
            strm.RRR_RRINDEX, 
            strm.RRR_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RRR_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5RELIABILITYRANKINGRANKS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RRR_PK=src.RRR_PK) OR (target.RRR_PK IS NULL AND src.RRR_PK IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.RRR_COLOR=src.RRR_COLOR, 
                    target.RRR_CRITICALITY=src.RRR_CRITICALITY, 
                    target.RRR_ICON=src.RRR_ICON, 
                    target.RRR_ICONPATH=src.RRR_ICONPATH, 
                    target.RRR_LASTSAVED=src.RRR_LASTSAVED, 
                    target.RRR_MAXVALUE=src.RRR_MAXVALUE, 
                    target.RRR_MINVALUE=src.RRR_MINVALUE, 
                    target.RRR_ORG=src.RRR_ORG, 
                    target.RRR_PK=src.RRR_PK, 
                    target.RRR_RELIABILITYRANKING=src.RRR_RELIABILITYRANKING, 
                    target.RRR_REVISION=src.RRR_REVISION, 
                    target.RRR_RRINDEX=src.RRR_RRINDEX, 
                    target.RRR_UPDATECOUNT=src.RRR_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    RRR_COLOR, 
                    RRR_CRITICALITY, 
                    RRR_ICON, 
                    RRR_ICONPATH, 
                    RRR_LASTSAVED, 
                    RRR_MAXVALUE, 
                    RRR_MINVALUE, 
                    RRR_ORG, 
                    RRR_PK, 
                    RRR_RELIABILITYRANKING, 
                    RRR_REVISION, 
                    RRR_RRINDEX, 
                    RRR_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.RRR_COLOR, 
                    src.RRR_CRITICALITY, 
                    src.RRR_ICON, 
                    src.RRR_ICONPATH, 
                    src.RRR_LASTSAVED, 
                    src.RRR_MAXVALUE, 
                    src.RRR_MINVALUE, 
                    src.RRR_ORG, 
                    src.RRR_PK, 
                    src.RRR_RELIABILITYRANKING, 
                    src.RRR_REVISION, 
                    src.RRR_RRINDEX, 
                    src.RRR_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5RELIABILITYRANKINGRANKS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.RRR_COLOR, 
            strm.RRR_CRITICALITY, 
            strm.RRR_ICON, 
            strm.RRR_ICONPATH, 
            strm.RRR_LASTSAVED, 
            strm.RRR_MAXVALUE, 
            strm.RRR_MINVALUE, 
            strm.RRR_ORG, 
            strm.RRR_PK, 
            strm.RRR_RELIABILITYRANKING, 
            strm.RRR_REVISION, 
            strm.RRR_RRINDEX, 
            strm.RRR_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.RRR_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5RELIABILITYRANKINGRANKS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.RRR_PK=src.RRR_PK) OR (target.RRR_PK IS NULL AND src.RRR_PK IS NULL)) 
                when matched then update set
                    target.RRR_COLOR=src.RRR_COLOR, 
                    target.RRR_CRITICALITY=src.RRR_CRITICALITY, 
                    target.RRR_ICON=src.RRR_ICON, 
                    target.RRR_ICONPATH=src.RRR_ICONPATH, 
                    target.RRR_LASTSAVED=src.RRR_LASTSAVED, 
                    target.RRR_MAXVALUE=src.RRR_MAXVALUE, 
                    target.RRR_MINVALUE=src.RRR_MINVALUE, 
                    target.RRR_ORG=src.RRR_ORG, 
                    target.RRR_PK=src.RRR_PK, 
                    target.RRR_RELIABILITYRANKING=src.RRR_RELIABILITYRANKING, 
                    target.RRR_REVISION=src.RRR_REVISION, 
                    target.RRR_RRINDEX=src.RRR_RRINDEX, 
                    target.RRR_UPDATECOUNT=src.RRR_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( RRR_COLOR, RRR_CRITICALITY, RRR_ICON, RRR_ICONPATH, RRR_LASTSAVED, RRR_MAXVALUE, RRR_MINVALUE, RRR_ORG, RRR_PK, RRR_RELIABILITYRANKING, RRR_REVISION, RRR_RRINDEX, RRR_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.RRR_COLOR, 
                    src.RRR_CRITICALITY, 
                    src.RRR_ICON, 
                    src.RRR_ICONPATH, 
                    src.RRR_LASTSAVED, 
                    src.RRR_MAXVALUE, 
                    src.RRR_MINVALUE, 
                    src.RRR_ORG, 
                    src.RRR_PK, 
                    src.RRR_RELIABILITYRANKING, 
                    src.RRR_REVISION, 
                    src.RRR_RRINDEX, 
                    src.RRR_UPDATECOUNT, 
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