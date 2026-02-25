create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLBILLING_ACCTSERVPOSITIONDELETED()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLBILLING_ACCTSERVPOSITIONDELETED_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLBILLING_ACCTSERVPOSITIONDELETED as target using (SELECT * FROM (SELECT 
            strm.ACCOUNTSERVICEKEY, 
            strm.ACCOUNTSERVICEPOSITIONKEY, 
            strm.ACCTSERVPOSITIONDELETEDKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDRKEY, 
            strm.ASSETTYPE, 
            strm.CONSUMPTIONPERCENTAGE, 
            strm.DELETED, 
            strm.GRAPHUSAGEFLAG, 
            strm.HIDEREADINGSONBILL, 
            strm.METERREGISTERUSE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MOVEINREADINGKEY, 
            strm.MOVEOUTREADINGKEY, 
            strm.ORIGADDBY, 
            strm.ORIGADDDTTM, 
            strm.ORIGMODBY, 
            strm.ORIGMODDTTM, 
            strm.POSITION, 
            strm.STARTDTTM, 
            strm.STOPDTTM, 
            strm.SUBTRACTIVEFLAG, 
            strm.USEDAYSRDS, 
            strm.USEUOM, 
            strm.USGHISTINBILLOUTPUT, 
            strm.VARIATION_ID, 
            strm.WINTERAVG, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ACCTSERVPOSITIONDELETEDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLBILLING_ACCTSERVPOSITIONDELETED as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ACCTSERVPOSITIONDELETEDKEY=src.ACCTSERVPOSITIONDELETEDKEY) OR (target.ACCTSERVPOSITIONDELETEDKEY IS NULL AND src.ACCTSERVPOSITIONDELETEDKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCOUNTSERVICEKEY=src.ACCOUNTSERVICEKEY, 
                    target.ACCOUNTSERVICEPOSITIONKEY=src.ACCOUNTSERVICEPOSITIONKEY, 
                    target.ACCTSERVPOSITIONDELETEDKEY=src.ACCTSERVPOSITIONDELETEDKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDRKEY=src.ADDRKEY, 
                    target.ASSETTYPE=src.ASSETTYPE, 
                    target.CONSUMPTIONPERCENTAGE=src.CONSUMPTIONPERCENTAGE, 
                    target.DELETED=src.DELETED, 
                    target.GRAPHUSAGEFLAG=src.GRAPHUSAGEFLAG, 
                    target.HIDEREADINGSONBILL=src.HIDEREADINGSONBILL, 
                    target.METERREGISTERUSE=src.METERREGISTERUSE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MOVEINREADINGKEY=src.MOVEINREADINGKEY, 
                    target.MOVEOUTREADINGKEY=src.MOVEOUTREADINGKEY, 
                    target.ORIGADDBY=src.ORIGADDBY, 
                    target.ORIGADDDTTM=src.ORIGADDDTTM, 
                    target.ORIGMODBY=src.ORIGMODBY, 
                    target.ORIGMODDTTM=src.ORIGMODDTTM, 
                    target.POSITION=src.POSITION, 
                    target.STARTDTTM=src.STARTDTTM, 
                    target.STOPDTTM=src.STOPDTTM, 
                    target.SUBTRACTIVEFLAG=src.SUBTRACTIVEFLAG, 
                    target.USEDAYSRDS=src.USEDAYSRDS, 
                    target.USEUOM=src.USEUOM, 
                    target.USGHISTINBILLOUTPUT=src.USGHISTINBILLOUTPUT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WINTERAVG=src.WINTERAVG, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCOUNTSERVICEKEY, 
                    ACCOUNTSERVICEPOSITIONKEY, 
                    ACCTSERVPOSITIONDELETEDKEY, 
                    ADDBY, 
                    ADDDTTM, 
                    ADDRKEY, 
                    ASSETTYPE, 
                    CONSUMPTIONPERCENTAGE, 
                    DELETED, 
                    GRAPHUSAGEFLAG, 
                    HIDEREADINGSONBILL, 
                    METERREGISTERUSE, 
                    MODBY, 
                    MODDTTM, 
                    MOVEINREADINGKEY, 
                    MOVEOUTREADINGKEY, 
                    ORIGADDBY, 
                    ORIGADDDTTM, 
                    ORIGMODBY, 
                    ORIGMODDTTM, 
                    POSITION, 
                    STARTDTTM, 
                    STOPDTTM, 
                    SUBTRACTIVEFLAG, 
                    USEDAYSRDS, 
                    USEUOM, 
                    USGHISTINBILLOUTPUT, 
                    VARIATION_ID, 
                    WINTERAVG, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCOUNTSERVICEKEY, 
                    src.ACCOUNTSERVICEPOSITIONKEY, 
                    src.ACCTSERVPOSITIONDELETEDKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDRKEY, 
                    src.ASSETTYPE, 
                    src.CONSUMPTIONPERCENTAGE, 
                    src.DELETED, 
                    src.GRAPHUSAGEFLAG, 
                    src.HIDEREADINGSONBILL, 
                    src.METERREGISTERUSE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MOVEINREADINGKEY, 
                    src.MOVEOUTREADINGKEY, 
                    src.ORIGADDBY, 
                    src.ORIGADDDTTM, 
                    src.ORIGMODBY, 
                    src.ORIGMODDTTM, 
                    src.POSITION, 
                    src.STARTDTTM, 
                    src.STOPDTTM, 
                    src.SUBTRACTIVEFLAG, 
                    src.USEDAYSRDS, 
                    src.USEUOM, 
                    src.USGHISTINBILLOUTPUT, 
                    src.VARIATION_ID, 
                    src.WINTERAVG,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLBILLING_ACCTSERVPOSITIONDELETED as target using (
                SELECT * FROM (SELECT 
            strm.ACCOUNTSERVICEKEY, 
            strm.ACCOUNTSERVICEPOSITIONKEY, 
            strm.ACCTSERVPOSITIONDELETEDKEY, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDRKEY, 
            strm.ASSETTYPE, 
            strm.CONSUMPTIONPERCENTAGE, 
            strm.DELETED, 
            strm.GRAPHUSAGEFLAG, 
            strm.HIDEREADINGSONBILL, 
            strm.METERREGISTERUSE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MOVEINREADINGKEY, 
            strm.MOVEOUTREADINGKEY, 
            strm.ORIGADDBY, 
            strm.ORIGADDDTTM, 
            strm.ORIGMODBY, 
            strm.ORIGMODDTTM, 
            strm.POSITION, 
            strm.STARTDTTM, 
            strm.STOPDTTM, 
            strm.SUBTRACTIVEFLAG, 
            strm.USEDAYSRDS, 
            strm.USEUOM, 
            strm.USGHISTINBILLOUTPUT, 
            strm.VARIATION_ID, 
            strm.WINTERAVG, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ACCTSERVPOSITIONDELETEDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLBILLING_ACCTSERVPOSITIONDELETED as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ACCTSERVPOSITIONDELETEDKEY=src.ACCTSERVPOSITIONDELETEDKEY) OR (target.ACCTSERVPOSITIONDELETEDKEY IS NULL AND src.ACCTSERVPOSITIONDELETEDKEY IS NULL)) 
                when matched then update set
                    target.ACCOUNTSERVICEKEY=src.ACCOUNTSERVICEKEY, 
                    target.ACCOUNTSERVICEPOSITIONKEY=src.ACCOUNTSERVICEPOSITIONKEY, 
                    target.ACCTSERVPOSITIONDELETEDKEY=src.ACCTSERVPOSITIONDELETEDKEY, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDRKEY=src.ADDRKEY, 
                    target.ASSETTYPE=src.ASSETTYPE, 
                    target.CONSUMPTIONPERCENTAGE=src.CONSUMPTIONPERCENTAGE, 
                    target.DELETED=src.DELETED, 
                    target.GRAPHUSAGEFLAG=src.GRAPHUSAGEFLAG, 
                    target.HIDEREADINGSONBILL=src.HIDEREADINGSONBILL, 
                    target.METERREGISTERUSE=src.METERREGISTERUSE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MOVEINREADINGKEY=src.MOVEINREADINGKEY, 
                    target.MOVEOUTREADINGKEY=src.MOVEOUTREADINGKEY, 
                    target.ORIGADDBY=src.ORIGADDBY, 
                    target.ORIGADDDTTM=src.ORIGADDDTTM, 
                    target.ORIGMODBY=src.ORIGMODBY, 
                    target.ORIGMODDTTM=src.ORIGMODDTTM, 
                    target.POSITION=src.POSITION, 
                    target.STARTDTTM=src.STARTDTTM, 
                    target.STOPDTTM=src.STOPDTTM, 
                    target.SUBTRACTIVEFLAG=src.SUBTRACTIVEFLAG, 
                    target.USEDAYSRDS=src.USEDAYSRDS, 
                    target.USEUOM=src.USEUOM, 
                    target.USGHISTINBILLOUTPUT=src.USGHISTINBILLOUTPUT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WINTERAVG=src.WINTERAVG, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCOUNTSERVICEKEY, ACCOUNTSERVICEPOSITIONKEY, ACCTSERVPOSITIONDELETEDKEY, ADDBY, ADDDTTM, ADDRKEY, ASSETTYPE, CONSUMPTIONPERCENTAGE, DELETED, GRAPHUSAGEFLAG, HIDEREADINGSONBILL, METERREGISTERUSE, MODBY, MODDTTM, MOVEINREADINGKEY, MOVEOUTREADINGKEY, ORIGADDBY, ORIGADDDTTM, ORIGMODBY, ORIGMODDTTM, POSITION, STARTDTTM, STOPDTTM, SUBTRACTIVEFLAG, USEDAYSRDS, USEUOM, USGHISTINBILLOUTPUT, VARIATION_ID, WINTERAVG, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCOUNTSERVICEKEY, 
                    src.ACCOUNTSERVICEPOSITIONKEY, 
                    src.ACCTSERVPOSITIONDELETEDKEY, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDRKEY, 
                    src.ASSETTYPE, 
                    src.CONSUMPTIONPERCENTAGE, 
                    src.DELETED, 
                    src.GRAPHUSAGEFLAG, 
                    src.HIDEREADINGSONBILL, 
                    src.METERREGISTERUSE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MOVEINREADINGKEY, 
                    src.MOVEOUTREADINGKEY, 
                    src.ORIGADDBY, 
                    src.ORIGADDDTTM, 
                    src.ORIGMODBY, 
                    src.ORIGMODDTTM, 
                    src.POSITION, 
                    src.STARTDTTM, 
                    src.STOPDTTM, 
                    src.SUBTRACTIVEFLAG, 
                    src.USEDAYSRDS, 
                    src.USEUOM, 
                    src.USGHISTINBILLOUTPUT, 
                    src.VARIATION_ID, 
                    src.WINTERAVG,     
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