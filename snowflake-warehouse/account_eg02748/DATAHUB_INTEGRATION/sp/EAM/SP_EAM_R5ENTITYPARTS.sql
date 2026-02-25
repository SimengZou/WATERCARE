create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5ENTITYPARTS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5ENTITYPARTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5ENTITYPARTS as target using (SELECT * FROM (SELECT 
            strm.EPA_ASMLEVEL, 
            strm.EPA_CODE, 
            strm.EPA_COMMENT, 
            strm.EPA_COMPLEVEL, 
            strm.EPA_ENTITY, 
            strm.EPA_LASTSAVED, 
            strm.EPA_PART, 
            strm.EPA_PARTLOCATION, 
            strm.EPA_PART_ORG, 
            strm.EPA_PK, 
            strm.EPA_QTY, 
            strm.EPA_RENTITY, 
            strm.EPA_RTYPE, 
            strm.EPA_SYSLEVEL, 
            strm.EPA_TYPE, 
            strm.EPA_UOM, 
            strm.EPA_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.EPA_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ENTITYPARTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.EPA_PK=src.EPA_PK) OR (target.EPA_PK IS NULL AND src.EPA_PK IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.EPA_ASMLEVEL=src.EPA_ASMLEVEL, 
                    target.EPA_CODE=src.EPA_CODE, 
                    target.EPA_COMMENT=src.EPA_COMMENT, 
                    target.EPA_COMPLEVEL=src.EPA_COMPLEVEL, 
                    target.EPA_ENTITY=src.EPA_ENTITY, 
                    target.EPA_LASTSAVED=src.EPA_LASTSAVED, 
                    target.EPA_PART=src.EPA_PART, 
                    target.EPA_PARTLOCATION=src.EPA_PARTLOCATION, 
                    target.EPA_PART_ORG=src.EPA_PART_ORG, 
                    target.EPA_PK=src.EPA_PK, 
                    target.EPA_QTY=src.EPA_QTY, 
                    target.EPA_RENTITY=src.EPA_RENTITY, 
                    target.EPA_RTYPE=src.EPA_RTYPE, 
                    target.EPA_SYSLEVEL=src.EPA_SYSLEVEL, 
                    target.EPA_TYPE=src.EPA_TYPE, 
                    target.EPA_UOM=src.EPA_UOM, 
                    target.EPA_UPDATECOUNT=src.EPA_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    EPA_ASMLEVEL, 
                    EPA_CODE, 
                    EPA_COMMENT, 
                    EPA_COMPLEVEL, 
                    EPA_ENTITY, 
                    EPA_LASTSAVED, 
                    EPA_PART, 
                    EPA_PARTLOCATION, 
                    EPA_PART_ORG, 
                    EPA_PK, 
                    EPA_QTY, 
                    EPA_RENTITY, 
                    EPA_RTYPE, 
                    EPA_SYSLEVEL, 
                    EPA_TYPE, 
                    EPA_UOM, 
                    EPA_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.EPA_ASMLEVEL, 
                    src.EPA_CODE, 
                    src.EPA_COMMENT, 
                    src.EPA_COMPLEVEL, 
                    src.EPA_ENTITY, 
                    src.EPA_LASTSAVED, 
                    src.EPA_PART, 
                    src.EPA_PARTLOCATION, 
                    src.EPA_PART_ORG, 
                    src.EPA_PK, 
                    src.EPA_QTY, 
                    src.EPA_RENTITY, 
                    src.EPA_RTYPE, 
                    src.EPA_SYSLEVEL, 
                    src.EPA_TYPE, 
                    src.EPA_UOM, 
                    src.EPA_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5ENTITYPARTS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.EPA_ASMLEVEL, 
            strm.EPA_CODE, 
            strm.EPA_COMMENT, 
            strm.EPA_COMPLEVEL, 
            strm.EPA_ENTITY, 
            strm.EPA_LASTSAVED, 
            strm.EPA_PART, 
            strm.EPA_PARTLOCATION, 
            strm.EPA_PART_ORG, 
            strm.EPA_PK, 
            strm.EPA_QTY, 
            strm.EPA_RENTITY, 
            strm.EPA_RTYPE, 
            strm.EPA_SYSLEVEL, 
            strm.EPA_TYPE, 
            strm.EPA_UOM, 
            strm.EPA_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.EPA_PK ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ENTITYPARTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.EPA_PK=src.EPA_PK) OR (target.EPA_PK IS NULL AND src.EPA_PK IS NULL)) 
                when matched then update set
                    target.EPA_ASMLEVEL=src.EPA_ASMLEVEL, 
                    target.EPA_CODE=src.EPA_CODE, 
                    target.EPA_COMMENT=src.EPA_COMMENT, 
                    target.EPA_COMPLEVEL=src.EPA_COMPLEVEL, 
                    target.EPA_ENTITY=src.EPA_ENTITY, 
                    target.EPA_LASTSAVED=src.EPA_LASTSAVED, 
                    target.EPA_PART=src.EPA_PART, 
                    target.EPA_PARTLOCATION=src.EPA_PARTLOCATION, 
                    target.EPA_PART_ORG=src.EPA_PART_ORG, 
                    target.EPA_PK=src.EPA_PK, 
                    target.EPA_QTY=src.EPA_QTY, 
                    target.EPA_RENTITY=src.EPA_RENTITY, 
                    target.EPA_RTYPE=src.EPA_RTYPE, 
                    target.EPA_SYSLEVEL=src.EPA_SYSLEVEL, 
                    target.EPA_TYPE=src.EPA_TYPE, 
                    target.EPA_UOM=src.EPA_UOM, 
                    target.EPA_UPDATECOUNT=src.EPA_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( EPA_ASMLEVEL, EPA_CODE, EPA_COMMENT, EPA_COMPLEVEL, EPA_ENTITY, EPA_LASTSAVED, EPA_PART, EPA_PARTLOCATION, EPA_PART_ORG, EPA_PK, EPA_QTY, EPA_RENTITY, EPA_RTYPE, EPA_SYSLEVEL, EPA_TYPE, EPA_UOM, EPA_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.EPA_ASMLEVEL, 
                    src.EPA_CODE, 
                    src.EPA_COMMENT, 
                    src.EPA_COMPLEVEL, 
                    src.EPA_ENTITY, 
                    src.EPA_LASTSAVED, 
                    src.EPA_PART, 
                    src.EPA_PARTLOCATION, 
                    src.EPA_PART_ORG, 
                    src.EPA_PK, 
                    src.EPA_QTY, 
                    src.EPA_RENTITY, 
                    src.EPA_RTYPE, 
                    src.EPA_SYSLEVEL, 
                    src.EPA_TYPE, 
                    src.EPA_UOM, 
                    src.EPA_UPDATECOUNT, 
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