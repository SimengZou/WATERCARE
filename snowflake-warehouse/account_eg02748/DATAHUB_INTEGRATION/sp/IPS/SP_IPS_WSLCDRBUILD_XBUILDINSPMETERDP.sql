create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLCDRBUILD_XBUILDINSPMETERDP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLCDRBUILD_XBUILDINSPMETERDP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLCDRBUILD_XBUILDINSPMETERDP as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APBLDGINSPDTLKEY, 
            strm.ARBORIST, 
            strm.BACKFLOWREQUIRED, 
            strm.BACKFLOWTYPE, 
            strm.CRITICALLINES, 
            strm.DELETED, 
            strm.DRAWINGNO, 
            strm.ESTIMATEDBYWSL, 
            strm.ESTTOTAL, 
            strm.FIREMETERID, 
            strm.FIREMETERINSTALLDATE, 
            strm.FIREMETERINSTALLEDBY, 
            strm.FIREMETERLOCATION, 
            strm.FIREMETERSIZE, 
            strm.INDUSTRY, 
            strm.MAGFLOWPOWERTYPE, 
            strm.MAGFLOWREQUIRED, 
            strm.METERBOXSIZE, 
            strm.METERBOXTYPE, 
            strm.METERID, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PEAKFLOW, 
            strm.PIPEMATERIAL, 
            strm.PRICINGPLAN, 
            strm.REMOTEREADER, 
            strm.SIZEMETER, 
            strm.SPRINKLERSUPPLYPIPESIZE, 
            strm.STRAINER, 
            strm.TMP, 
            strm.VARIATION_ID, 
            strm.WATERMAINLOC, 
            strm.WATERMAINSIZE, 
            strm.WATERNEWMETERSIZE, 
            strm.WORKTYPE, 
            strm.XBUILDINSPMETERDPKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XBUILDINSPMETERDPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRBUILD_XBUILDINSPMETERDP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XBUILDINSPMETERDPKEY=src.XBUILDINSPMETERDPKEY) OR (target.XBUILDINSPMETERDPKEY IS NULL AND src.XBUILDINSPMETERDPKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APBLDGINSPDTLKEY=src.APBLDGINSPDTLKEY, 
                    target.ARBORIST=src.ARBORIST, 
                    target.BACKFLOWREQUIRED=src.BACKFLOWREQUIRED, 
                    target.BACKFLOWTYPE=src.BACKFLOWTYPE, 
                    target.CRITICALLINES=src.CRITICALLINES, 
                    target.DELETED=src.DELETED, 
                    target.DRAWINGNO=src.DRAWINGNO, 
                    target.ESTIMATEDBYWSL=src.ESTIMATEDBYWSL, 
                    target.ESTTOTAL=src.ESTTOTAL, 
                    target.FIREMETERID=src.FIREMETERID, 
                    target.FIREMETERINSTALLDATE=src.FIREMETERINSTALLDATE, 
                    target.FIREMETERINSTALLEDBY=src.FIREMETERINSTALLEDBY, 
                    target.FIREMETERLOCATION=src.FIREMETERLOCATION, 
                    target.FIREMETERSIZE=src.FIREMETERSIZE, 
                    target.INDUSTRY=src.INDUSTRY, 
                    target.MAGFLOWPOWERTYPE=src.MAGFLOWPOWERTYPE, 
                    target.MAGFLOWREQUIRED=src.MAGFLOWREQUIRED, 
                    target.METERBOXSIZE=src.METERBOXSIZE, 
                    target.METERBOXTYPE=src.METERBOXTYPE, 
                    target.METERID=src.METERID, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PEAKFLOW=src.PEAKFLOW, 
                    target.PIPEMATERIAL=src.PIPEMATERIAL, 
                    target.PRICINGPLAN=src.PRICINGPLAN, 
                    target.REMOTEREADER=src.REMOTEREADER, 
                    target.SIZEMETER=src.SIZEMETER, 
                    target.SPRINKLERSUPPLYPIPESIZE=src.SPRINKLERSUPPLYPIPESIZE, 
                    target.STRAINER=src.STRAINER, 
                    target.TMP=src.TMP, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WATERMAINLOC=src.WATERMAINLOC, 
                    target.WATERMAINSIZE=src.WATERMAINSIZE, 
                    target.WATERNEWMETERSIZE=src.WATERNEWMETERSIZE, 
                    target.WORKTYPE=src.WORKTYPE, 
                    target.XBUILDINSPMETERDPKEY=src.XBUILDINSPMETERDPKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    APBLDGINSPDTLKEY, 
                    ARBORIST, 
                    BACKFLOWREQUIRED, 
                    BACKFLOWTYPE, 
                    CRITICALLINES, 
                    DELETED, 
                    DRAWINGNO, 
                    ESTIMATEDBYWSL, 
                    ESTTOTAL, 
                    FIREMETERID, 
                    FIREMETERINSTALLDATE, 
                    FIREMETERINSTALLEDBY, 
                    FIREMETERLOCATION, 
                    FIREMETERSIZE, 
                    INDUSTRY, 
                    MAGFLOWPOWERTYPE, 
                    MAGFLOWREQUIRED, 
                    METERBOXSIZE, 
                    METERBOXTYPE, 
                    METERID, 
                    MODBY, 
                    MODDTTM, 
                    PEAKFLOW, 
                    PIPEMATERIAL, 
                    PRICINGPLAN, 
                    REMOTEREADER, 
                    SIZEMETER, 
                    SPRINKLERSUPPLYPIPESIZE, 
                    STRAINER, 
                    TMP, 
                    VARIATION_ID, 
                    WATERMAINLOC, 
                    WATERMAINSIZE, 
                    WATERNEWMETERSIZE, 
                    WORKTYPE, 
                    XBUILDINSPMETERDPKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APBLDGINSPDTLKEY, 
                    src.ARBORIST, 
                    src.BACKFLOWREQUIRED, 
                    src.BACKFLOWTYPE, 
                    src.CRITICALLINES, 
                    src.DELETED, 
                    src.DRAWINGNO, 
                    src.ESTIMATEDBYWSL, 
                    src.ESTTOTAL, 
                    src.FIREMETERID, 
                    src.FIREMETERINSTALLDATE, 
                    src.FIREMETERINSTALLEDBY, 
                    src.FIREMETERLOCATION, 
                    src.FIREMETERSIZE, 
                    src.INDUSTRY, 
                    src.MAGFLOWPOWERTYPE, 
                    src.MAGFLOWREQUIRED, 
                    src.METERBOXSIZE, 
                    src.METERBOXTYPE, 
                    src.METERID, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PEAKFLOW, 
                    src.PIPEMATERIAL, 
                    src.PRICINGPLAN, 
                    src.REMOTEREADER, 
                    src.SIZEMETER, 
                    src.SPRINKLERSUPPLYPIPESIZE, 
                    src.STRAINER, 
                    src.TMP, 
                    src.VARIATION_ID, 
                    src.WATERMAINLOC, 
                    src.WATERMAINSIZE, 
                    src.WATERNEWMETERSIZE, 
                    src.WORKTYPE, 
                    src.XBUILDINSPMETERDPKEY,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRBUILD_XBUILDINSPMETERDP as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APBLDGINSPDTLKEY, 
            strm.ARBORIST, 
            strm.BACKFLOWREQUIRED, 
            strm.BACKFLOWTYPE, 
            strm.CRITICALLINES, 
            strm.DELETED, 
            strm.DRAWINGNO, 
            strm.ESTIMATEDBYWSL, 
            strm.ESTTOTAL, 
            strm.FIREMETERID, 
            strm.FIREMETERINSTALLDATE, 
            strm.FIREMETERINSTALLEDBY, 
            strm.FIREMETERLOCATION, 
            strm.FIREMETERSIZE, 
            strm.INDUSTRY, 
            strm.MAGFLOWPOWERTYPE, 
            strm.MAGFLOWREQUIRED, 
            strm.METERBOXSIZE, 
            strm.METERBOXTYPE, 
            strm.METERID, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PEAKFLOW, 
            strm.PIPEMATERIAL, 
            strm.PRICINGPLAN, 
            strm.REMOTEREADER, 
            strm.SIZEMETER, 
            strm.SPRINKLERSUPPLYPIPESIZE, 
            strm.STRAINER, 
            strm.TMP, 
            strm.VARIATION_ID, 
            strm.WATERMAINLOC, 
            strm.WATERMAINSIZE, 
            strm.WATERNEWMETERSIZE, 
            strm.WORKTYPE, 
            strm.XBUILDINSPMETERDPKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XBUILDINSPMETERDPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRBUILD_XBUILDINSPMETERDP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XBUILDINSPMETERDPKEY=src.XBUILDINSPMETERDPKEY) OR (target.XBUILDINSPMETERDPKEY IS NULL AND src.XBUILDINSPMETERDPKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APBLDGINSPDTLKEY=src.APBLDGINSPDTLKEY, 
                    target.ARBORIST=src.ARBORIST, 
                    target.BACKFLOWREQUIRED=src.BACKFLOWREQUIRED, 
                    target.BACKFLOWTYPE=src.BACKFLOWTYPE, 
                    target.CRITICALLINES=src.CRITICALLINES, 
                    target.DELETED=src.DELETED, 
                    target.DRAWINGNO=src.DRAWINGNO, 
                    target.ESTIMATEDBYWSL=src.ESTIMATEDBYWSL, 
                    target.ESTTOTAL=src.ESTTOTAL, 
                    target.FIREMETERID=src.FIREMETERID, 
                    target.FIREMETERINSTALLDATE=src.FIREMETERINSTALLDATE, 
                    target.FIREMETERINSTALLEDBY=src.FIREMETERINSTALLEDBY, 
                    target.FIREMETERLOCATION=src.FIREMETERLOCATION, 
                    target.FIREMETERSIZE=src.FIREMETERSIZE, 
                    target.INDUSTRY=src.INDUSTRY, 
                    target.MAGFLOWPOWERTYPE=src.MAGFLOWPOWERTYPE, 
                    target.MAGFLOWREQUIRED=src.MAGFLOWREQUIRED, 
                    target.METERBOXSIZE=src.METERBOXSIZE, 
                    target.METERBOXTYPE=src.METERBOXTYPE, 
                    target.METERID=src.METERID, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PEAKFLOW=src.PEAKFLOW, 
                    target.PIPEMATERIAL=src.PIPEMATERIAL, 
                    target.PRICINGPLAN=src.PRICINGPLAN, 
                    target.REMOTEREADER=src.REMOTEREADER, 
                    target.SIZEMETER=src.SIZEMETER, 
                    target.SPRINKLERSUPPLYPIPESIZE=src.SPRINKLERSUPPLYPIPESIZE, 
                    target.STRAINER=src.STRAINER, 
                    target.TMP=src.TMP, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WATERMAINLOC=src.WATERMAINLOC, 
                    target.WATERMAINSIZE=src.WATERMAINSIZE, 
                    target.WATERNEWMETERSIZE=src.WATERNEWMETERSIZE, 
                    target.WORKTYPE=src.WORKTYPE, 
                    target.XBUILDINSPMETERDPKEY=src.XBUILDINSPMETERDPKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, APBLDGINSPDTLKEY, ARBORIST, BACKFLOWREQUIRED, BACKFLOWTYPE, CRITICALLINES, DELETED, DRAWINGNO, ESTIMATEDBYWSL, ESTTOTAL, FIREMETERID, FIREMETERINSTALLDATE, FIREMETERINSTALLEDBY, FIREMETERLOCATION, FIREMETERSIZE, INDUSTRY, MAGFLOWPOWERTYPE, MAGFLOWREQUIRED, METERBOXSIZE, METERBOXTYPE, METERID, MODBY, MODDTTM, PEAKFLOW, PIPEMATERIAL, PRICINGPLAN, REMOTEREADER, SIZEMETER, SPRINKLERSUPPLYPIPESIZE, STRAINER, TMP, VARIATION_ID, WATERMAINLOC, WATERMAINSIZE, WATERNEWMETERSIZE, WORKTYPE, XBUILDINSPMETERDPKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APBLDGINSPDTLKEY, 
                    src.ARBORIST, 
                    src.BACKFLOWREQUIRED, 
                    src.BACKFLOWTYPE, 
                    src.CRITICALLINES, 
                    src.DELETED, 
                    src.DRAWINGNO, 
                    src.ESTIMATEDBYWSL, 
                    src.ESTTOTAL, 
                    src.FIREMETERID, 
                    src.FIREMETERINSTALLDATE, 
                    src.FIREMETERINSTALLEDBY, 
                    src.FIREMETERLOCATION, 
                    src.FIREMETERSIZE, 
                    src.INDUSTRY, 
                    src.MAGFLOWPOWERTYPE, 
                    src.MAGFLOWREQUIRED, 
                    src.METERBOXSIZE, 
                    src.METERBOXTYPE, 
                    src.METERID, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PEAKFLOW, 
                    src.PIPEMATERIAL, 
                    src.PRICINGPLAN, 
                    src.REMOTEREADER, 
                    src.SIZEMETER, 
                    src.SPRINKLERSUPPLYPIPESIZE, 
                    src.STRAINER, 
                    src.TMP, 
                    src.VARIATION_ID, 
                    src.WATERMAINLOC, 
                    src.WATERMAINSIZE, 
                    src.WATERNEWMETERSIZE, 
                    src.WORKTYPE, 
                    src.XBUILDINSPMETERDPKEY,     
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