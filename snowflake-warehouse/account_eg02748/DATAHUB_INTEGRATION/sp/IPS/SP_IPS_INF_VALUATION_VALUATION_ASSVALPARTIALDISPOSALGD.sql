create or replace procedure DATAHUB_INTEGRATION.SP_IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSALGD()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSALGD_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSALGD as target using (SELECT * FROM (SELECT 
            strm.ACCUMDEPRECAFTERDISPOSAL, 
            strm.ACCUMDEPRECAMOUNT, 
            strm.ACCUMDEPRECBEFOREDISPOSAL, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ASSVALPARTIALDISPOSALGDKEY, 
            strm.ASSVALPARTIALDISPOSALKEY, 
            strm.BOOKID, 
            strm.CURRENTCOSTAFTERDISPOSAL, 
            strm.CURRENTCOSTB4DISPOSAL, 
            strm.DATALAKE_DELETED, 
            strm.DISPOSALDATE, 
            strm.DISPOSALNUMBER, 
            strm.DISPOSALVALUE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.OPENINGQUANTITY, 
            strm.PERCENTAGEAFTERDISPOSAL, 
            strm.PERCENTAGEB4DISPOSAL, 
            strm.PERCENTAGEDISPOSED, 
            strm.PROFITLOSS, 
            strm.QUANTITYAFTERDISPOSAL, 
            strm.QUANTITYB4DISPOSAL, 
            strm.QUANTITYDISPOSED, 
            strm.UNITTYPE, 
            strm.VALUATIONKEY, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ASSVALPARTIALDISPOSALGDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSALGD as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ASSVALPARTIALDISPOSALGDKEY=src.ASSVALPARTIALDISPOSALGDKEY) OR (target.ASSVALPARTIALDISPOSALGDKEY IS NULL AND src.ASSVALPARTIALDISPOSALGDKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCUMDEPRECAFTERDISPOSAL=src.ACCUMDEPRECAFTERDISPOSAL, 
                    target.ACCUMDEPRECAMOUNT=src.ACCUMDEPRECAMOUNT, 
                    target.ACCUMDEPRECBEFOREDISPOSAL=src.ACCUMDEPRECBEFOREDISPOSAL, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ASSVALPARTIALDISPOSALGDKEY=src.ASSVALPARTIALDISPOSALGDKEY, 
                    target.ASSVALPARTIALDISPOSALKEY=src.ASSVALPARTIALDISPOSALKEY, 
                    target.BOOKID=src.BOOKID, 
                    target.CURRENTCOSTAFTERDISPOSAL=src.CURRENTCOSTAFTERDISPOSAL, 
                    target.CURRENTCOSTB4DISPOSAL=src.CURRENTCOSTB4DISPOSAL, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DISPOSALDATE=src.DISPOSALDATE, 
                    target.DISPOSALNUMBER=src.DISPOSALNUMBER, 
                    target.DISPOSALVALUE=src.DISPOSALVALUE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.OPENINGQUANTITY=src.OPENINGQUANTITY, 
                    target.PERCENTAGEAFTERDISPOSAL=src.PERCENTAGEAFTERDISPOSAL, 
                    target.PERCENTAGEB4DISPOSAL=src.PERCENTAGEB4DISPOSAL, 
                    target.PERCENTAGEDISPOSED=src.PERCENTAGEDISPOSED, 
                    target.PROFITLOSS=src.PROFITLOSS, 
                    target.QUANTITYAFTERDISPOSAL=src.QUANTITYAFTERDISPOSAL, 
                    target.QUANTITYB4DISPOSAL=src.QUANTITYB4DISPOSAL, 
                    target.QUANTITYDISPOSED=src.QUANTITYDISPOSED, 
                    target.UNITTYPE=src.UNITTYPE, 
                    target.VALUATIONKEY=src.VALUATIONKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCUMDEPRECAFTERDISPOSAL, 
                    ACCUMDEPRECAMOUNT, 
                    ACCUMDEPRECBEFOREDISPOSAL, 
                    ADDBY, 
                    ADDDTTM, 
                    ASSVALPARTIALDISPOSALGDKEY, 
                    ASSVALPARTIALDISPOSALKEY, 
                    BOOKID, 
                    CURRENTCOSTAFTERDISPOSAL, 
                    CURRENTCOSTB4DISPOSAL, 
                    DATALAKE_DELETED, 
                    DISPOSALDATE, 
                    DISPOSALNUMBER, 
                    DISPOSALVALUE, 
                    MODBY, 
                    MODDTTM, 
                    OPENINGQUANTITY, 
                    PERCENTAGEAFTERDISPOSAL, 
                    PERCENTAGEB4DISPOSAL, 
                    PERCENTAGEDISPOSED, 
                    PROFITLOSS, 
                    QUANTITYAFTERDISPOSAL, 
                    QUANTITYB4DISPOSAL, 
                    QUANTITYDISPOSED, 
                    UNITTYPE, 
                    VALUATIONKEY, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCUMDEPRECAFTERDISPOSAL, 
                    src.ACCUMDEPRECAMOUNT, 
                    src.ACCUMDEPRECBEFOREDISPOSAL, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ASSVALPARTIALDISPOSALGDKEY, 
                    src.ASSVALPARTIALDISPOSALKEY, 
                    src.BOOKID, 
                    src.CURRENTCOSTAFTERDISPOSAL, 
                    src.CURRENTCOSTB4DISPOSAL, 
                    src.DATALAKE_DELETED, 
                    src.DISPOSALDATE, 
                    src.DISPOSALNUMBER, 
                    src.DISPOSALVALUE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.OPENINGQUANTITY, 
                    src.PERCENTAGEAFTERDISPOSAL, 
                    src.PERCENTAGEB4DISPOSAL, 
                    src.PERCENTAGEDISPOSED, 
                    src.PROFITLOSS, 
                    src.QUANTITYAFTERDISPOSAL, 
                    src.QUANTITYB4DISPOSAL, 
                    src.QUANTITYDISPOSED, 
                    src.UNITTYPE, 
                    src.VALUATIONKEY, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSALGD as target using (
                SELECT * FROM (SELECT 
            strm.ACCUMDEPRECAFTERDISPOSAL, 
            strm.ACCUMDEPRECAMOUNT, 
            strm.ACCUMDEPRECBEFOREDISPOSAL, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ASSVALPARTIALDISPOSALGDKEY, 
            strm.ASSVALPARTIALDISPOSALKEY, 
            strm.BOOKID, 
            strm.CURRENTCOSTAFTERDISPOSAL, 
            strm.CURRENTCOSTB4DISPOSAL, 
            strm.DATALAKE_DELETED, 
            strm.DISPOSALDATE, 
            strm.DISPOSALNUMBER, 
            strm.DISPOSALVALUE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.OPENINGQUANTITY, 
            strm.PERCENTAGEAFTERDISPOSAL, 
            strm.PERCENTAGEB4DISPOSAL, 
            strm.PERCENTAGEDISPOSED, 
            strm.PROFITLOSS, 
            strm.QUANTITYAFTERDISPOSAL, 
            strm.QUANTITYB4DISPOSAL, 
            strm.QUANTITYDISPOSED, 
            strm.UNITTYPE, 
            strm.VALUATIONKEY, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.ASSVALPARTIALDISPOSALGDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_INF_VALUATION_VALUATION_ASSVALPARTIALDISPOSALGD as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.ASSVALPARTIALDISPOSALGDKEY=src.ASSVALPARTIALDISPOSALGDKEY) OR (target.ASSVALPARTIALDISPOSALGDKEY IS NULL AND src.ASSVALPARTIALDISPOSALGDKEY IS NULL)) 
                when matched then update set
                    target.ACCUMDEPRECAFTERDISPOSAL=src.ACCUMDEPRECAFTERDISPOSAL, 
                    target.ACCUMDEPRECAMOUNT=src.ACCUMDEPRECAMOUNT, 
                    target.ACCUMDEPRECBEFOREDISPOSAL=src.ACCUMDEPRECBEFOREDISPOSAL, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ASSVALPARTIALDISPOSALGDKEY=src.ASSVALPARTIALDISPOSALGDKEY, 
                    target.ASSVALPARTIALDISPOSALKEY=src.ASSVALPARTIALDISPOSALKEY, 
                    target.BOOKID=src.BOOKID, 
                    target.CURRENTCOSTAFTERDISPOSAL=src.CURRENTCOSTAFTERDISPOSAL, 
                    target.CURRENTCOSTB4DISPOSAL=src.CURRENTCOSTB4DISPOSAL, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DISPOSALDATE=src.DISPOSALDATE, 
                    target.DISPOSALNUMBER=src.DISPOSALNUMBER, 
                    target.DISPOSALVALUE=src.DISPOSALVALUE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.OPENINGQUANTITY=src.OPENINGQUANTITY, 
                    target.PERCENTAGEAFTERDISPOSAL=src.PERCENTAGEAFTERDISPOSAL, 
                    target.PERCENTAGEB4DISPOSAL=src.PERCENTAGEB4DISPOSAL, 
                    target.PERCENTAGEDISPOSED=src.PERCENTAGEDISPOSED, 
                    target.PROFITLOSS=src.PROFITLOSS, 
                    target.QUANTITYAFTERDISPOSAL=src.QUANTITYAFTERDISPOSAL, 
                    target.QUANTITYB4DISPOSAL=src.QUANTITYB4DISPOSAL, 
                    target.QUANTITYDISPOSED=src.QUANTITYDISPOSED, 
                    target.UNITTYPE=src.UNITTYPE, 
                    target.VALUATIONKEY=src.VALUATIONKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCUMDEPRECAFTERDISPOSAL, ACCUMDEPRECAMOUNT, ACCUMDEPRECBEFOREDISPOSAL, ADDBY, ADDDTTM, ASSVALPARTIALDISPOSALGDKEY, ASSVALPARTIALDISPOSALKEY, BOOKID, CURRENTCOSTAFTERDISPOSAL, CURRENTCOSTB4DISPOSAL, DATALAKE_DELETED, DISPOSALDATE, DISPOSALNUMBER, DISPOSALVALUE, MODBY, MODDTTM, OPENINGQUANTITY, PERCENTAGEAFTERDISPOSAL, PERCENTAGEB4DISPOSAL, PERCENTAGEDISPOSED, PROFITLOSS, QUANTITYAFTERDISPOSAL, QUANTITYB4DISPOSAL, QUANTITYDISPOSED, UNITTYPE, VALUATIONKEY, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCUMDEPRECAFTERDISPOSAL, 
                    src.ACCUMDEPRECAMOUNT, 
                    src.ACCUMDEPRECBEFOREDISPOSAL, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ASSVALPARTIALDISPOSALGDKEY, 
                    src.ASSVALPARTIALDISPOSALKEY, 
                    src.BOOKID, 
                    src.CURRENTCOSTAFTERDISPOSAL, 
                    src.CURRENTCOSTB4DISPOSAL, 
                    src.DATALAKE_DELETED, 
                    src.DISPOSALDATE, 
                    src.DISPOSALNUMBER, 
                    src.DISPOSALVALUE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.OPENINGQUANTITY, 
                    src.PERCENTAGEAFTERDISPOSAL, 
                    src.PERCENTAGEB4DISPOSAL, 
                    src.PERCENTAGEDISPOSED, 
                    src.PROFITLOSS, 
                    src.QUANTITYAFTERDISPOSAL, 
                    src.QUANTITYB4DISPOSAL, 
                    src.QUANTITYDISPOSED, 
                    src.UNITTYPE, 
                    src.VALUATIONKEY, 
                    src.VARIATION_ID,     
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