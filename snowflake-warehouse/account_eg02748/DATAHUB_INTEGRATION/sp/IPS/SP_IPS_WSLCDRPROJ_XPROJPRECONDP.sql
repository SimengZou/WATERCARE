create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLCDRPROJ_XPROJPRECONDP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLCDRPROJ_XPROJPRECONDP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLCDRPROJ_XPROJPRECONDP as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPROJAPPLDTLKEY, 
            strm.BILLPAYER, 
            strm.CONSTRUCTIONSTAGE, 
            strm.CT, 
            strm.DATALAKE_DELETED, 
            strm.DP, 
            strm.EPANUMBER, 
            strm.LOT, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NEIGHBOURHOOD, 
            strm.STAGE, 
            strm.SUPERLOT, 
            strm.VARIATION_ID, 
            strm.XPROJPRECONDPKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XPROJPRECONDPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRPROJ_XPROJPRECONDP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XPROJPRECONDPKEY=src.XPROJPRECONDPKEY) OR (target.XPROJPRECONDPKEY IS NULL AND src.XPROJPRECONDPKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPROJAPPLDTLKEY=src.APPROJAPPLDTLKEY, 
                    target.BILLPAYER=src.BILLPAYER, 
                    target.CONSTRUCTIONSTAGE=src.CONSTRUCTIONSTAGE, 
                    target.CT=src.CT, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DP=src.DP, 
                    target.EPANUMBER=src.EPANUMBER, 
                    target.LOT=src.LOT, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NEIGHBOURHOOD=src.NEIGHBOURHOOD, 
                    target.STAGE=src.STAGE, 
                    target.SUPERLOT=src.SUPERLOT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.XPROJPRECONDPKEY=src.XPROJPRECONDPKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    APPROJAPPLDTLKEY, 
                    BILLPAYER, 
                    CONSTRUCTIONSTAGE, 
                    CT, 
                    DATALAKE_DELETED, 
                    DP, 
                    EPANUMBER, 
                    LOT, 
                    MODBY, 
                    MODDTTM, 
                    NEIGHBOURHOOD, 
                    STAGE, 
                    SUPERLOT, 
                    VARIATION_ID, 
                    XPROJPRECONDPKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPROJAPPLDTLKEY, 
                    src.BILLPAYER, 
                    src.CONSTRUCTIONSTAGE, 
                    src.CT, 
                    src.DATALAKE_DELETED, 
                    src.DP, 
                    src.EPANUMBER, 
                    src.LOT, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NEIGHBOURHOOD, 
                    src.STAGE, 
                    src.SUPERLOT, 
                    src.VARIATION_ID, 
                    src.XPROJPRECONDPKEY,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRPROJ_XPROJPRECONDP as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APPROJAPPLDTLKEY, 
            strm.BILLPAYER, 
            strm.CONSTRUCTIONSTAGE, 
            strm.CT, 
            strm.DATALAKE_DELETED, 
            strm.DP, 
            strm.EPANUMBER, 
            strm.LOT, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NEIGHBOURHOOD, 
            strm.STAGE, 
            strm.SUPERLOT, 
            strm.VARIATION_ID, 
            strm.XPROJPRECONDPKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XPROJPRECONDPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRPROJ_XPROJPRECONDP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XPROJPRECONDPKEY=src.XPROJPRECONDPKEY) OR (target.XPROJPRECONDPKEY IS NULL AND src.XPROJPRECONDPKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APPROJAPPLDTLKEY=src.APPROJAPPLDTLKEY, 
                    target.BILLPAYER=src.BILLPAYER, 
                    target.CONSTRUCTIONSTAGE=src.CONSTRUCTIONSTAGE, 
                    target.CT=src.CT, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DP=src.DP, 
                    target.EPANUMBER=src.EPANUMBER, 
                    target.LOT=src.LOT, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NEIGHBOURHOOD=src.NEIGHBOURHOOD, 
                    target.STAGE=src.STAGE, 
                    target.SUPERLOT=src.SUPERLOT, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.XPROJPRECONDPKEY=src.XPROJPRECONDPKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, APPROJAPPLDTLKEY, BILLPAYER, CONSTRUCTIONSTAGE, CT, DATALAKE_DELETED, DP, EPANUMBER, LOT, MODBY, MODDTTM, NEIGHBOURHOOD, STAGE, SUPERLOT, VARIATION_ID, XPROJPRECONDPKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APPROJAPPLDTLKEY, 
                    src.BILLPAYER, 
                    src.CONSTRUCTIONSTAGE, 
                    src.CT, 
                    src.DATALAKE_DELETED, 
                    src.DP, 
                    src.EPANUMBER, 
                    src.LOT, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NEIGHBOURHOOD, 
                    src.STAGE, 
                    src.SUPERLOT, 
                    src.VARIATION_ID, 
                    src.XPROJPRECONDPKEY,     
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