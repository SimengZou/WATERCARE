create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLVOC_VOCDETAILS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLVOC_VOCDETAILS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLVOC_VOCDETAILS as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.DELETED, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.SERVNO, 
            strm.VARIATION_ID, 
            strm.VCA, 
            strm.VCH, 
            strm.VCO, 
            strm.VFD, 
            strm.VFO, 
            strm.VFR, 
            strm.VOCDETAILSKEY, 
            strm.VON, 
            strm.VPA, 
            strm.VPB, 
            strm.VPC, 
            strm.VPI, 
            strm.VPL, 
            strm.VPM, 
            strm.VPO, 
            strm.VPP, 
            strm.VPT, 
            strm.VSA, 
            strm.VSE, 
            strm.VSH, 
            strm.VSI, 
            strm.VSK, 
            strm.VSL, 
            strm.VSO, 
            strm.VSQ, 
            strm.VSR, 
            strm.VWA, 
            strm.VWF, 
            strm.VWN, 
            strm.VWO, 
            strm.VWP, 
            strm.VWR, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.VOCDETAILSKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLVOC_VOCDETAILS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.VOCDETAILSKEY=src.VOCDETAILSKEY) OR (target.VOCDETAILSKEY IS NULL AND src.VOCDETAILSKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.DELETED=src.DELETED, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.SERVNO=src.SERVNO, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VCA=src.VCA, 
                    target.VCH=src.VCH, 
                    target.VCO=src.VCO, 
                    target.VFD=src.VFD, 
                    target.VFO=src.VFO, 
                    target.VFR=src.VFR, 
                    target.VOCDETAILSKEY=src.VOCDETAILSKEY, 
                    target.VON=src.VON, 
                    target.VPA=src.VPA, 
                    target.VPB=src.VPB, 
                    target.VPC=src.VPC, 
                    target.VPI=src.VPI, 
                    target.VPL=src.VPL, 
                    target.VPM=src.VPM, 
                    target.VPO=src.VPO, 
                    target.VPP=src.VPP, 
                    target.VPT=src.VPT, 
                    target.VSA=src.VSA, 
                    target.VSE=src.VSE, 
                    target.VSH=src.VSH, 
                    target.VSI=src.VSI, 
                    target.VSK=src.VSK, 
                    target.VSL=src.VSL, 
                    target.VSO=src.VSO, 
                    target.VSQ=src.VSQ, 
                    target.VSR=src.VSR, 
                    target.VWA=src.VWA, 
                    target.VWF=src.VWF, 
                    target.VWN=src.VWN, 
                    target.VWO=src.VWO, 
                    target.VWP=src.VWP, 
                    target.VWR=src.VWR, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    DELETED, 
                    MODBY, 
                    MODDTTM, 
                    SERVNO, 
                    VARIATION_ID, 
                    VCA, 
                    VCH, 
                    VCO, 
                    VFD, 
                    VFO, 
                    VFR, 
                    VOCDETAILSKEY, 
                    VON, 
                    VPA, 
                    VPB, 
                    VPC, 
                    VPI, 
                    VPL, 
                    VPM, 
                    VPO, 
                    VPP, 
                    VPT, 
                    VSA, 
                    VSE, 
                    VSH, 
                    VSI, 
                    VSK, 
                    VSL, 
                    VSO, 
                    VSQ, 
                    VSR, 
                    VWA, 
                    VWF, 
                    VWN, 
                    VWO, 
                    VWP, 
                    VWR, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.DELETED, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.SERVNO, 
                    src.VARIATION_ID, 
                    src.VCA, 
                    src.VCH, 
                    src.VCO, 
                    src.VFD, 
                    src.VFO, 
                    src.VFR, 
                    src.VOCDETAILSKEY, 
                    src.VON, 
                    src.VPA, 
                    src.VPB, 
                    src.VPC, 
                    src.VPI, 
                    src.VPL, 
                    src.VPM, 
                    src.VPO, 
                    src.VPP, 
                    src.VPT, 
                    src.VSA, 
                    src.VSE, 
                    src.VSH, 
                    src.VSI, 
                    src.VSK, 
                    src.VSL, 
                    src.VSO, 
                    src.VSQ, 
                    src.VSR, 
                    src.VWA, 
                    src.VWF, 
                    src.VWN, 
                    src.VWO, 
                    src.VWP, 
                    src.VWR,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLVOC_VOCDETAILS as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.DELETED, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.SERVNO, 
            strm.VARIATION_ID, 
            strm.VCA, 
            strm.VCH, 
            strm.VCO, 
            strm.VFD, 
            strm.VFO, 
            strm.VFR, 
            strm.VOCDETAILSKEY, 
            strm.VON, 
            strm.VPA, 
            strm.VPB, 
            strm.VPC, 
            strm.VPI, 
            strm.VPL, 
            strm.VPM, 
            strm.VPO, 
            strm.VPP, 
            strm.VPT, 
            strm.VSA, 
            strm.VSE, 
            strm.VSH, 
            strm.VSI, 
            strm.VSK, 
            strm.VSL, 
            strm.VSO, 
            strm.VSQ, 
            strm.VSR, 
            strm.VWA, 
            strm.VWF, 
            strm.VWN, 
            strm.VWO, 
            strm.VWP, 
            strm.VWR, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.VOCDETAILSKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLVOC_VOCDETAILS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.VOCDETAILSKEY=src.VOCDETAILSKEY) OR (target.VOCDETAILSKEY IS NULL AND src.VOCDETAILSKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.DELETED=src.DELETED, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.SERVNO=src.SERVNO, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VCA=src.VCA, 
                    target.VCH=src.VCH, 
                    target.VCO=src.VCO, 
                    target.VFD=src.VFD, 
                    target.VFO=src.VFO, 
                    target.VFR=src.VFR, 
                    target.VOCDETAILSKEY=src.VOCDETAILSKEY, 
                    target.VON=src.VON, 
                    target.VPA=src.VPA, 
                    target.VPB=src.VPB, 
                    target.VPC=src.VPC, 
                    target.VPI=src.VPI, 
                    target.VPL=src.VPL, 
                    target.VPM=src.VPM, 
                    target.VPO=src.VPO, 
                    target.VPP=src.VPP, 
                    target.VPT=src.VPT, 
                    target.VSA=src.VSA, 
                    target.VSE=src.VSE, 
                    target.VSH=src.VSH, 
                    target.VSI=src.VSI, 
                    target.VSK=src.VSK, 
                    target.VSL=src.VSL, 
                    target.VSO=src.VSO, 
                    target.VSQ=src.VSQ, 
                    target.VSR=src.VSR, 
                    target.VWA=src.VWA, 
                    target.VWF=src.VWF, 
                    target.VWN=src.VWN, 
                    target.VWO=src.VWO, 
                    target.VWP=src.VWP, 
                    target.VWR=src.VWR, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, DELETED, MODBY, MODDTTM, SERVNO, VARIATION_ID, VCA, VCH, VCO, VFD, VFO, VFR, VOCDETAILSKEY, VON, VPA, VPB, VPC, VPI, VPL, VPM, VPO, VPP, VPT, VSA, VSE, VSH, VSI, VSK, VSL, VSO, VSQ, VSR, VWA, VWF, VWN, VWO, VWP, VWR, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.DELETED, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.SERVNO, 
                    src.VARIATION_ID, 
                    src.VCA, 
                    src.VCH, 
                    src.VCO, 
                    src.VFD, 
                    src.VFO, 
                    src.VFR, 
                    src.VOCDETAILSKEY, 
                    src.VON, 
                    src.VPA, 
                    src.VPB, 
                    src.VPC, 
                    src.VPI, 
                    src.VPL, 
                    src.VPM, 
                    src.VPO, 
                    src.VPP, 
                    src.VPT, 
                    src.VSA, 
                    src.VSE, 
                    src.VSH, 
                    src.VSI, 
                    src.VSK, 
                    src.VSL, 
                    src.VSO, 
                    src.VSQ, 
                    src.VSR, 
                    src.VWA, 
                    src.VWF, 
                    src.VWN, 
                    src.VWO, 
                    src.VWP, 
                    src.VWR,     
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