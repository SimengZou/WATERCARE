create or replace procedure DATAHUB_INTEGRATION.SP_IPS_INF_VALUATION_VALUATION_ASSVALRESERVEGD()
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
                            INSERT INTO LANDING_ERROR.IPS_RAW_ERROR
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_INF_VALUATION_VALUATION_ASSVALRESERVEGD_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_INF_VALUATION_VALUATION_ASSVALRESERVEGD as target using (SELECT * FROM (SELECT 
            strm.ACUMDEPB4REVAL, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ASSVALRESERVEGDKEY, 
            strm.ASSVALRESERVEKEY, 
            strm.BALSHTBOOKVAL, 
            strm.CAPEXP, 
            strm.CLOSERESERVE, 
            strm.COMPKEY, 
            strm.DATALAKE_DELETED, 
            strm.DISPDATE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MOVEMENT, 
            strm.OPENEXPLIFE, 
            strm.OPENFYDATE, 
            strm.OPENREPLCOST, 
            strm.OPENRESERVE, 
            strm.OPENRESIDLIFE, 
            strm.OPENWDV, 
            strm.REMLIFE, 
            strm.RESERVEDATE, 
            strm.REVALREPLCOST, 
            strm.RUNNO, 
            strm.SOURCETYPE, 
            strm.VALKEY, 
            strm.VALUETODISPOSE, 
            strm.VARIATION_ID, 
            strm.WDV, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ASSVALRESERVEGDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_INF_VALUATION_VALUATION_ASSVALRESERVEGD as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ASSVALRESERVEGDKEY=src.ASSVALRESERVEGDKEY) OR (target.ASSVALRESERVEGDKEY IS NULL AND src.ASSVALRESERVEGDKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACUMDEPB4REVAL=src.ACUMDEPB4REVAL, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ASSVALRESERVEGDKEY=src.ASSVALRESERVEGDKEY, 
                    target.ASSVALRESERVEKEY=src.ASSVALRESERVEKEY, 
                    target.BALSHTBOOKVAL=src.BALSHTBOOKVAL, 
                    target.CAPEXP=src.CAPEXP, 
                    target.CLOSERESERVE=src.CLOSERESERVE, 
                    target.COMPKEY=src.COMPKEY, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DISPDATE=src.DISPDATE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MOVEMENT=src.MOVEMENT, 
                    target.OPENEXPLIFE=src.OPENEXPLIFE, 
                    target.OPENFYDATE=src.OPENFYDATE, 
                    target.OPENREPLCOST=src.OPENREPLCOST, 
                    target.OPENRESERVE=src.OPENRESERVE, 
                    target.OPENRESIDLIFE=src.OPENRESIDLIFE, 
                    target.OPENWDV=src.OPENWDV, 
                    target.REMLIFE=src.REMLIFE, 
                    target.RESERVEDATE=src.RESERVEDATE, 
                    target.REVALREPLCOST=src.REVALREPLCOST, 
                    target.RUNNO=src.RUNNO, 
                    target.SOURCETYPE=src.SOURCETYPE, 
                    target.VALKEY=src.VALKEY, 
                    target.VALUETODISPOSE=src.VALUETODISPOSE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WDV=src.WDV, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACUMDEPB4REVAL, 
                    ADDBY, 
                    ADDDTTM, 
                    ASSVALRESERVEGDKEY, 
                    ASSVALRESERVEKEY, 
                    BALSHTBOOKVAL, 
                    CAPEXP, 
                    CLOSERESERVE, 
                    COMPKEY, 
                    DATALAKE_DELETED, 
                    DISPDATE, 
                    MODBY, 
                    MODDTTM, 
                    MOVEMENT, 
                    OPENEXPLIFE, 
                    OPENFYDATE, 
                    OPENREPLCOST, 
                    OPENRESERVE, 
                    OPENRESIDLIFE, 
                    OPENWDV, 
                    REMLIFE, 
                    RESERVEDATE, 
                    REVALREPLCOST, 
                    RUNNO, 
                    SOURCETYPE, 
                    VALKEY, 
                    VALUETODISPOSE, 
                    VARIATION_ID, 
                    WDV, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACUMDEPB4REVAL, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ASSVALRESERVEGDKEY, 
                    src.ASSVALRESERVEKEY, 
                    src.BALSHTBOOKVAL, 
                    src.CAPEXP, 
                    src.CLOSERESERVE, 
                    src.COMPKEY, 
                    src.DATALAKE_DELETED, 
                    src.DISPDATE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MOVEMENT, 
                    src.OPENEXPLIFE, 
                    src.OPENFYDATE, 
                    src.OPENREPLCOST, 
                    src.OPENRESERVE, 
                    src.OPENRESIDLIFE, 
                    src.OPENWDV, 
                    src.REMLIFE, 
                    src.RESERVEDATE, 
                    src.REVALREPLCOST, 
                    src.RUNNO, 
                    src.SOURCETYPE, 
                    src.VALKEY, 
                    src.VALUETODISPOSE, 
                    src.VARIATION_ID, 
                    src.WDV,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_INF_VALUATION_VALUATION_ASSVALRESERVEGD as target using (
                SELECT * FROM (SELECT 
            strm.ACUMDEPB4REVAL, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ASSVALRESERVEGDKEY, 
            strm.ASSVALRESERVEKEY, 
            strm.BALSHTBOOKVAL, 
            strm.CAPEXP, 
            strm.CLOSERESERVE, 
            strm.COMPKEY, 
            strm.DATALAKE_DELETED, 
            strm.DISPDATE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MOVEMENT, 
            strm.OPENEXPLIFE, 
            strm.OPENFYDATE, 
            strm.OPENREPLCOST, 
            strm.OPENRESERVE, 
            strm.OPENRESIDLIFE, 
            strm.OPENWDV, 
            strm.REMLIFE, 
            strm.RESERVEDATE, 
            strm.REVALREPLCOST, 
            strm.RUNNO, 
            strm.SOURCETYPE, 
            strm.VALKEY, 
            strm.VALUETODISPOSE, 
            strm.VARIATION_ID, 
            strm.WDV, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ASSVALRESERVEGDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_INF_VALUATION_VALUATION_ASSVALRESERVEGD as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ASSVALRESERVEGDKEY=src.ASSVALRESERVEGDKEY) OR (target.ASSVALRESERVEGDKEY IS NULL AND src.ASSVALRESERVEGDKEY IS NULL)) 
                when matched then update set
                    target.ACUMDEPB4REVAL=src.ACUMDEPB4REVAL, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ASSVALRESERVEGDKEY=src.ASSVALRESERVEGDKEY, 
                    target.ASSVALRESERVEKEY=src.ASSVALRESERVEKEY, 
                    target.BALSHTBOOKVAL=src.BALSHTBOOKVAL, 
                    target.CAPEXP=src.CAPEXP, 
                    target.CLOSERESERVE=src.CLOSERESERVE, 
                    target.COMPKEY=src.COMPKEY, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DISPDATE=src.DISPDATE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MOVEMENT=src.MOVEMENT, 
                    target.OPENEXPLIFE=src.OPENEXPLIFE, 
                    target.OPENFYDATE=src.OPENFYDATE, 
                    target.OPENREPLCOST=src.OPENREPLCOST, 
                    target.OPENRESERVE=src.OPENRESERVE, 
                    target.OPENRESIDLIFE=src.OPENRESIDLIFE, 
                    target.OPENWDV=src.OPENWDV, 
                    target.REMLIFE=src.REMLIFE, 
                    target.RESERVEDATE=src.RESERVEDATE, 
                    target.REVALREPLCOST=src.REVALREPLCOST, 
                    target.RUNNO=src.RUNNO, 
                    target.SOURCETYPE=src.SOURCETYPE, 
                    target.VALKEY=src.VALKEY, 
                    target.VALUETODISPOSE=src.VALUETODISPOSE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WDV=src.WDV, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACUMDEPB4REVAL, ADDBY, ADDDTTM, ASSVALRESERVEGDKEY, ASSVALRESERVEKEY, BALSHTBOOKVAL, CAPEXP, CLOSERESERVE, COMPKEY, DATALAKE_DELETED, DISPDATE, MODBY, MODDTTM, MOVEMENT, OPENEXPLIFE, OPENFYDATE, OPENREPLCOST, OPENRESERVE, OPENRESIDLIFE, OPENWDV, REMLIFE, RESERVEDATE, REVALREPLCOST, RUNNO, SOURCETYPE, VALKEY, VALUETODISPOSE, VARIATION_ID, WDV, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACUMDEPB4REVAL, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ASSVALRESERVEGDKEY, 
                    src.ASSVALRESERVEKEY, 
                    src.BALSHTBOOKVAL, 
                    src.CAPEXP, 
                    src.CLOSERESERVE, 
                    src.COMPKEY, 
                    src.DATALAKE_DELETED, 
                    src.DISPDATE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MOVEMENT, 
                    src.OPENEXPLIFE, 
                    src.OPENFYDATE, 
                    src.OPENREPLCOST, 
                    src.OPENRESERVE, 
                    src.OPENRESIDLIFE, 
                    src.OPENWDV, 
                    src.REMLIFE, 
                    src.RESERVEDATE, 
                    src.REVALREPLCOST, 
                    src.RUNNO, 
                    src.SOURCETYPE, 
                    src.VALKEY, 
                    src.VALUETODISPOSE, 
                    src.VARIATION_ID, 
                    src.WDV,     
                    src.ETL_SEQUENCE_NUMBER,
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