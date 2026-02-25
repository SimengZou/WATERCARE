create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5HAZARDPRECAUTIONS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5HAZARDPRECAUTIONS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5HAZARDPRECAUTIONS as target using (SELECT * FROM (SELECT 
            strm.HZP_HAZARD, 
            strm.HZP_HAZARD_ORG, 
            strm.HZP_LASTSAVED, 
            strm.HZP_PRECAUTION, 
            strm.HZP_PRECAUTION_ORG, 
            strm.HZP_REVISION, 
            strm.HZP_SEQUENCE, 
            strm.HZP_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.HZP_HAZARD,
            strm.HZP_HAZARD_ORG,
            strm.HZP_REVISION,
            strm.HZP_PRECAUTION,
            strm.HZP_PRECAUTION_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5HAZARDPRECAUTIONS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.HZP_HAZARD=src.HZP_HAZARD) OR (target.HZP_HAZARD IS NULL AND src.HZP_HAZARD IS NULL)) AND
            ((target.HZP_HAZARD_ORG=src.HZP_HAZARD_ORG) OR (target.HZP_HAZARD_ORG IS NULL AND src.HZP_HAZARD_ORG IS NULL)) AND
            ((target.HZP_REVISION=src.HZP_REVISION) OR (target.HZP_REVISION IS NULL AND src.HZP_REVISION IS NULL)) AND
            ((target.HZP_PRECAUTION=src.HZP_PRECAUTION) OR (target.HZP_PRECAUTION IS NULL AND src.HZP_PRECAUTION IS NULL)) AND
            ((target.HZP_PRECAUTION_ORG=src.HZP_PRECAUTION_ORG) OR (target.HZP_PRECAUTION_ORG IS NULL AND src.HZP_PRECAUTION_ORG IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.HZP_HAZARD=src.HZP_HAZARD, 
                    target.HZP_HAZARD_ORG=src.HZP_HAZARD_ORG, 
                    target.HZP_LASTSAVED=src.HZP_LASTSAVED, 
                    target.HZP_PRECAUTION=src.HZP_PRECAUTION, 
                    target.HZP_PRECAUTION_ORG=src.HZP_PRECAUTION_ORG, 
                    target.HZP_REVISION=src.HZP_REVISION, 
                    target.HZP_SEQUENCE=src.HZP_SEQUENCE, 
                    target.HZP_UPDATECOUNT=src.HZP_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    HZP_HAZARD, 
                    HZP_HAZARD_ORG, 
                    HZP_LASTSAVED, 
                    HZP_PRECAUTION, 
                    HZP_PRECAUTION_ORG, 
                    HZP_REVISION, 
                    HZP_SEQUENCE, 
                    HZP_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.HZP_HAZARD, 
                    src.HZP_HAZARD_ORG, 
                    src.HZP_LASTSAVED, 
                    src.HZP_PRECAUTION, 
                    src.HZP_PRECAUTION_ORG, 
                    src.HZP_REVISION, 
                    src.HZP_SEQUENCE, 
                    src.HZP_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5HAZARDPRECAUTIONS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.HZP_HAZARD, 
            strm.HZP_HAZARD_ORG, 
            strm.HZP_LASTSAVED, 
            strm.HZP_PRECAUTION, 
            strm.HZP_PRECAUTION_ORG, 
            strm.HZP_REVISION, 
            strm.HZP_SEQUENCE, 
            strm.HZP_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.HZP_HAZARD,
            strm.HZP_HAZARD_ORG,
            strm.HZP_REVISION,
            strm.HZP_PRECAUTION,
            strm.HZP_PRECAUTION_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5HAZARDPRECAUTIONS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.HZP_HAZARD=src.HZP_HAZARD) OR (target.HZP_HAZARD IS NULL AND src.HZP_HAZARD IS NULL)) AND
            ((target.HZP_HAZARD_ORG=src.HZP_HAZARD_ORG) OR (target.HZP_HAZARD_ORG IS NULL AND src.HZP_HAZARD_ORG IS NULL)) AND
            ((target.HZP_REVISION=src.HZP_REVISION) OR (target.HZP_REVISION IS NULL AND src.HZP_REVISION IS NULL)) AND
            ((target.HZP_PRECAUTION=src.HZP_PRECAUTION) OR (target.HZP_PRECAUTION IS NULL AND src.HZP_PRECAUTION IS NULL)) AND
            ((target.HZP_PRECAUTION_ORG=src.HZP_PRECAUTION_ORG) OR (target.HZP_PRECAUTION_ORG IS NULL AND src.HZP_PRECAUTION_ORG IS NULL)) 
                when matched then update set
                    target.HZP_HAZARD=src.HZP_HAZARD, 
                    target.HZP_HAZARD_ORG=src.HZP_HAZARD_ORG, 
                    target.HZP_LASTSAVED=src.HZP_LASTSAVED, 
                    target.HZP_PRECAUTION=src.HZP_PRECAUTION, 
                    target.HZP_PRECAUTION_ORG=src.HZP_PRECAUTION_ORG, 
                    target.HZP_REVISION=src.HZP_REVISION, 
                    target.HZP_SEQUENCE=src.HZP_SEQUENCE, 
                    target.HZP_UPDATECOUNT=src.HZP_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( HZP_HAZARD, HZP_HAZARD_ORG, HZP_LASTSAVED, HZP_PRECAUTION, HZP_PRECAUTION_ORG, HZP_REVISION, HZP_SEQUENCE, HZP_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.HZP_HAZARD, 
                    src.HZP_HAZARD_ORG, 
                    src.HZP_LASTSAVED, 
                    src.HZP_PRECAUTION, 
                    src.HZP_PRECAUTION_ORG, 
                    src.HZP_REVISION, 
                    src.HZP_SEQUENCE, 
                    src.HZP_UPDATECOUNT, 
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