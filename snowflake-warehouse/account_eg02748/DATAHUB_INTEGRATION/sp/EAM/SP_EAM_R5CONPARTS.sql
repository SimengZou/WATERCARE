create or replace procedure DATAHUB_INTEGRATION.SP_EAM_R5CONPARTS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_EAM_R5CONPARTS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  TARGET_EAM.EAM_R5CONPARTS as target using (SELECT * FROM (SELECT 
            strm.CPA_CONTRACT, 
            strm.CPA_LASTSAVED, 
            strm.CPA_LEADTIME, 
            strm.CPA_MULTIPLY, 
            strm.CPA_PART, 
            strm.CPA_PART_ORG, 
            strm.CPA_PRICE, 
            strm.CPA_PURUOM, 
            strm.CPA_REF, 
            strm.CPA_SUPPLIER, 
            strm.CPA_SUPPLIER_ORG, 
            strm.CPA_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CPA_CONTRACT,
            strm.CPA_PART,
            strm.CPA_PART_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CONPARTS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CPA_CONTRACT=src.CPA_CONTRACT) OR (target.CPA_CONTRACT IS NULL AND src.CPA_CONTRACT IS NULL)) AND
            ((target.CPA_PART=src.CPA_PART) OR (target.CPA_PART IS NULL AND src.CPA_PART IS NULL)) AND
            ((target.CPA_PART_ORG=src.CPA_PART_ORG) OR (target.CPA_PART_ORG IS NULL AND src.CPA_PART_ORG IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.CPA_CONTRACT=src.CPA_CONTRACT, 
                    target.CPA_LASTSAVED=src.CPA_LASTSAVED, 
                    target.CPA_LEADTIME=src.CPA_LEADTIME, 
                    target.CPA_MULTIPLY=src.CPA_MULTIPLY, 
                    target.CPA_PART=src.CPA_PART, 
                    target.CPA_PART_ORG=src.CPA_PART_ORG, 
                    target.CPA_PRICE=src.CPA_PRICE, 
                    target.CPA_PURUOM=src.CPA_PURUOM, 
                    target.CPA_REF=src.CPA_REF, 
                    target.CPA_SUPPLIER=src.CPA_SUPPLIER, 
                    target.CPA_SUPPLIER_ORG=src.CPA_SUPPLIER_ORG, 
                    target.CPA_UPDATECOUNT=src.CPA_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    CPA_CONTRACT, 
                    CPA_LASTSAVED, 
                    CPA_LEADTIME, 
                    CPA_MULTIPLY, 
                    CPA_PART, 
                    CPA_PART_ORG, 
                    CPA_PRICE, 
                    CPA_PURUOM, 
                    CPA_REF, 
                    CPA_SUPPLIER, 
                    CPA_SUPPLIER_ORG, 
                    CPA_UPDATECOUNT, 
                    _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.CPA_CONTRACT, 
                    src.CPA_LASTSAVED, 
                    src.CPA_LEADTIME, 
                    src.CPA_MULTIPLY, 
                    src.CPA_PART, 
                    src.CPA_PART_ORG, 
                    src.CPA_PRICE, 
                    src.CPA_PURUOM, 
                    src.CPA_REF, 
                    src.CPA_SUPPLIER, 
                    src.CPA_SUPPLIER_ORG, 
                    src.CPA_UPDATECOUNT, 
                    src._DELETED,     
                    src.ETL_LASTSAVED,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into TARGET_HISTORY_EAM.EAM_R5CONPARTS_DELETED as target using (
                SELECT * FROM (SELECT 
            strm.CPA_CONTRACT, 
            strm.CPA_LASTSAVED, 
            strm.CPA_LEADTIME, 
            strm.CPA_MULTIPLY, 
            strm.CPA_PART, 
            strm.CPA_PART_ORG, 
            strm.CPA_PRICE, 
            strm.CPA_PURUOM, 
            strm.CPA_REF, 
            strm.CPA_SUPPLIER, 
            strm.CPA_SUPPLIER_ORG, 
            strm.CPA_UPDATECOUNT, 
            strm._DELETED, 
            strm.ETL_LASTSAVED,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.CPA_CONTRACT,
            strm.CPA_PART,
            strm.CPA_PART_ORG ORDER BY strm.ETL_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_EAM_R5CONPARTS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.CPA_CONTRACT=src.CPA_CONTRACT) OR (target.CPA_CONTRACT IS NULL AND src.CPA_CONTRACT IS NULL)) AND
            ((target.CPA_PART=src.CPA_PART) OR (target.CPA_PART IS NULL AND src.CPA_PART IS NULL)) AND
            ((target.CPA_PART_ORG=src.CPA_PART_ORG) OR (target.CPA_PART_ORG IS NULL AND src.CPA_PART_ORG IS NULL)) 
                when matched then update set
                    target.CPA_CONTRACT=src.CPA_CONTRACT, 
                    target.CPA_LASTSAVED=src.CPA_LASTSAVED, 
                    target.CPA_LEADTIME=src.CPA_LEADTIME, 
                    target.CPA_MULTIPLY=src.CPA_MULTIPLY, 
                    target.CPA_PART=src.CPA_PART, 
                    target.CPA_PART_ORG=src.CPA_PART_ORG, 
                    target.CPA_PRICE=src.CPA_PRICE, 
                    target.CPA_PURUOM=src.CPA_PURUOM, 
                    target.CPA_REF=src.CPA_REF, 
                    target.CPA_SUPPLIER=src.CPA_SUPPLIER, 
                    target.CPA_SUPPLIER_ORG=src.CPA_SUPPLIER_ORG, 
                    target.CPA_UPDATECOUNT=src.CPA_UPDATECOUNT, 
                    target._DELETED=src._DELETED, 
                    target.ETL_LASTSAVED=src.ETL_LASTSAVED,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( CPA_CONTRACT, CPA_LASTSAVED, CPA_LEADTIME, CPA_MULTIPLY, CPA_PART, CPA_PART_ORG, CPA_PRICE, CPA_PURUOM, CPA_REF, CPA_SUPPLIER, CPA_SUPPLIER_ORG, CPA_UPDATECOUNT, _DELETED, 
                    ETL_LASTSAVED,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.CPA_CONTRACT, 
                    src.CPA_LASTSAVED, 
                    src.CPA_LEADTIME, 
                    src.CPA_MULTIPLY, 
                    src.CPA_PART, 
                    src.CPA_PART_ORG, 
                    src.CPA_PRICE, 
                    src.CPA_PURUOM, 
                    src.CPA_REF, 
                    src.CPA_SUPPLIER, 
                    src.CPA_SUPPLIER_ORG, 
                    src.CPA_UPDATECOUNT, 
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