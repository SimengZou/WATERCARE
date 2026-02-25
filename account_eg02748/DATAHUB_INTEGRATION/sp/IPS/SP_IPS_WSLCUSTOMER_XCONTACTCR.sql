create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLCUSTOMER_XCONTACTCR()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLCUSTOMER_XCONTACTCR_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLCUSTOMER_XCONTACTCR as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.CNTCTKEY, 
            strm.COUNTRY, 
            strm.DELETED, 
            strm.ISRECORDUPDATED, 
            strm.LANDLINE1, 
            strm.LANDLINE1AREA, 
            strm.LANDLINE1NO, 
            strm.LANDLINE1TYPE, 
            strm.LANDLINE2, 
            strm.LANDLINE2AREA, 
            strm.LANDLINE2NO, 
            strm.LANDLINE2TYPE, 
            strm.MOBILE, 
            strm.MOBILE1AREA, 
            strm.MOBILE1NO, 
            strm.MOBILE2, 
            strm.MOBILE2AREA, 
            strm.MOBILE2NO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.OVERSEA, 
            strm.OVERSEAAREA, 
            strm.OVERSEANO, 
            strm.POSTCODE, 
            strm.PRIMARYEMAILFLAG, 
            strm.PRIMARYMOBILEFLAG, 
            strm.TOLLFREE, 
            strm.VARIATION_ID, 
            strm.XCONTACTCRKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XCONTACTCRKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCUSTOMER_XCONTACTCR as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XCONTACTCRKEY=src.XCONTACTCRKEY) OR (target.XCONTACTCRKEY IS NULL AND src.XCONTACTCRKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.CNTCTKEY=src.CNTCTKEY, 
                    target.COUNTRY=src.COUNTRY, 
                    target.DELETED=src.DELETED, 
                    target.ISRECORDUPDATED=src.ISRECORDUPDATED, 
                    target.LANDLINE1=src.LANDLINE1, 
                    target.LANDLINE1AREA=src.LANDLINE1AREA, 
                    target.LANDLINE1NO=src.LANDLINE1NO, 
                    target.LANDLINE1TYPE=src.LANDLINE1TYPE, 
                    target.LANDLINE2=src.LANDLINE2, 
                    target.LANDLINE2AREA=src.LANDLINE2AREA, 
                    target.LANDLINE2NO=src.LANDLINE2NO, 
                    target.LANDLINE2TYPE=src.LANDLINE2TYPE, 
                    target.MOBILE=src.MOBILE, 
                    target.MOBILE1AREA=src.MOBILE1AREA, 
                    target.MOBILE1NO=src.MOBILE1NO, 
                    target.MOBILE2=src.MOBILE2, 
                    target.MOBILE2AREA=src.MOBILE2AREA, 
                    target.MOBILE2NO=src.MOBILE2NO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.OVERSEA=src.OVERSEA, 
                    target.OVERSEAAREA=src.OVERSEAAREA, 
                    target.OVERSEANO=src.OVERSEANO, 
                    target.POSTCODE=src.POSTCODE, 
                    target.PRIMARYEMAILFLAG=src.PRIMARYEMAILFLAG, 
                    target.PRIMARYMOBILEFLAG=src.PRIMARYMOBILEFLAG, 
                    target.TOLLFREE=src.TOLLFREE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.XCONTACTCRKEY=src.XCONTACTCRKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    CNTCTKEY, 
                    COUNTRY, 
                    DELETED, 
                    ISRECORDUPDATED, 
                    LANDLINE1, 
                    LANDLINE1AREA, 
                    LANDLINE1NO, 
                    LANDLINE1TYPE, 
                    LANDLINE2, 
                    LANDLINE2AREA, 
                    LANDLINE2NO, 
                    LANDLINE2TYPE, 
                    MOBILE, 
                    MOBILE1AREA, 
                    MOBILE1NO, 
                    MOBILE2, 
                    MOBILE2AREA, 
                    MOBILE2NO, 
                    MODBY, 
                    MODDTTM, 
                    OVERSEA, 
                    OVERSEAAREA, 
                    OVERSEANO, 
                    POSTCODE, 
                    PRIMARYEMAILFLAG, 
                    PRIMARYMOBILEFLAG, 
                    TOLLFREE, 
                    VARIATION_ID, 
                    XCONTACTCRKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.CNTCTKEY, 
                    src.COUNTRY, 
                    src.DELETED, 
                    src.ISRECORDUPDATED, 
                    src.LANDLINE1, 
                    src.LANDLINE1AREA, 
                    src.LANDLINE1NO, 
                    src.LANDLINE1TYPE, 
                    src.LANDLINE2, 
                    src.LANDLINE2AREA, 
                    src.LANDLINE2NO, 
                    src.LANDLINE2TYPE, 
                    src.MOBILE, 
                    src.MOBILE1AREA, 
                    src.MOBILE1NO, 
                    src.MOBILE2, 
                    src.MOBILE2AREA, 
                    src.MOBILE2NO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.OVERSEA, 
                    src.OVERSEAAREA, 
                    src.OVERSEANO, 
                    src.POSTCODE, 
                    src.PRIMARYEMAILFLAG, 
                    src.PRIMARYMOBILEFLAG, 
                    src.TOLLFREE, 
                    src.VARIATION_ID, 
                    src.XCONTACTCRKEY,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCUSTOMER_XCONTACTCR as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.CNTCTKEY, 
            strm.COUNTRY, 
            strm.DELETED, 
            strm.ISRECORDUPDATED, 
            strm.LANDLINE1, 
            strm.LANDLINE1AREA, 
            strm.LANDLINE1NO, 
            strm.LANDLINE1TYPE, 
            strm.LANDLINE2, 
            strm.LANDLINE2AREA, 
            strm.LANDLINE2NO, 
            strm.LANDLINE2TYPE, 
            strm.MOBILE, 
            strm.MOBILE1AREA, 
            strm.MOBILE1NO, 
            strm.MOBILE2, 
            strm.MOBILE2AREA, 
            strm.MOBILE2NO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.OVERSEA, 
            strm.OVERSEAAREA, 
            strm.OVERSEANO, 
            strm.POSTCODE, 
            strm.PRIMARYEMAILFLAG, 
            strm.PRIMARYMOBILEFLAG, 
            strm.TOLLFREE, 
            strm.VARIATION_ID, 
            strm.XCONTACTCRKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XCONTACTCRKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCUSTOMER_XCONTACTCR as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XCONTACTCRKEY=src.XCONTACTCRKEY) OR (target.XCONTACTCRKEY IS NULL AND src.XCONTACTCRKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.CNTCTKEY=src.CNTCTKEY, 
                    target.COUNTRY=src.COUNTRY, 
                    target.DELETED=src.DELETED, 
                    target.ISRECORDUPDATED=src.ISRECORDUPDATED, 
                    target.LANDLINE1=src.LANDLINE1, 
                    target.LANDLINE1AREA=src.LANDLINE1AREA, 
                    target.LANDLINE1NO=src.LANDLINE1NO, 
                    target.LANDLINE1TYPE=src.LANDLINE1TYPE, 
                    target.LANDLINE2=src.LANDLINE2, 
                    target.LANDLINE2AREA=src.LANDLINE2AREA, 
                    target.LANDLINE2NO=src.LANDLINE2NO, 
                    target.LANDLINE2TYPE=src.LANDLINE2TYPE, 
                    target.MOBILE=src.MOBILE, 
                    target.MOBILE1AREA=src.MOBILE1AREA, 
                    target.MOBILE1NO=src.MOBILE1NO, 
                    target.MOBILE2=src.MOBILE2, 
                    target.MOBILE2AREA=src.MOBILE2AREA, 
                    target.MOBILE2NO=src.MOBILE2NO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.OVERSEA=src.OVERSEA, 
                    target.OVERSEAAREA=src.OVERSEAAREA, 
                    target.OVERSEANO=src.OVERSEANO, 
                    target.POSTCODE=src.POSTCODE, 
                    target.PRIMARYEMAILFLAG=src.PRIMARYEMAILFLAG, 
                    target.PRIMARYMOBILEFLAG=src.PRIMARYMOBILEFLAG, 
                    target.TOLLFREE=src.TOLLFREE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.XCONTACTCRKEY=src.XCONTACTCRKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, CNTCTKEY, COUNTRY, DELETED, ISRECORDUPDATED, LANDLINE1, LANDLINE1AREA, LANDLINE1NO, LANDLINE1TYPE, LANDLINE2, LANDLINE2AREA, LANDLINE2NO, LANDLINE2TYPE, MOBILE, MOBILE1AREA, MOBILE1NO, MOBILE2, MOBILE2AREA, MOBILE2NO, MODBY, MODDTTM, OVERSEA, OVERSEAAREA, OVERSEANO, POSTCODE, PRIMARYEMAILFLAG, PRIMARYMOBILEFLAG, TOLLFREE, VARIATION_ID, XCONTACTCRKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.CNTCTKEY, 
                    src.COUNTRY, 
                    src.DELETED, 
                    src.ISRECORDUPDATED, 
                    src.LANDLINE1, 
                    src.LANDLINE1AREA, 
                    src.LANDLINE1NO, 
                    src.LANDLINE1TYPE, 
                    src.LANDLINE2, 
                    src.LANDLINE2AREA, 
                    src.LANDLINE2NO, 
                    src.LANDLINE2TYPE, 
                    src.MOBILE, 
                    src.MOBILE1AREA, 
                    src.MOBILE1NO, 
                    src.MOBILE2, 
                    src.MOBILE2AREA, 
                    src.MOBILE2NO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.OVERSEA, 
                    src.OVERSEAAREA, 
                    src.OVERSEANO, 
                    src.POSTCODE, 
                    src.PRIMARYEMAILFLAG, 
                    src.PRIMARYMOBILEFLAG, 
                    src.TOLLFREE, 
                    src.VARIATION_ID, 
                    src.XCONTACTCRKEY,     
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