create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLPORTAL_XPORTALSR()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLPORTAL_XPORTALSR_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLPORTAL_XPORTALSR as target using (SELECT * FROM (SELECT 
            strm.ACCTNO, 
            strm.ACCTNOTADD, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDR1, 
            strm.ADDR2, 
            strm.CITY, 
            strm.COUNTRY, 
            strm.DACCTNAME, 
            strm.DATEFIXD, 
            strm.DAYPHN, 
            strm.DDACCTNO, 
            strm.DDACTYPE, 
            strm.DDBANK, 
            strm.DDBRANCH, 
            strm.DDMASKNO, 
            strm.DDMAXAMT, 
            strm.DELETED, 
            strm.EMAIL, 
            strm.EVEPHN, 
            strm.FEDBKTYE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NOOCC, 
            strm.POSTCODE, 
            strm.QUESTTOPC, 
            strm.SDLDATE, 
            strm.SERVNO, 
            strm.STATE, 
            strm.USERID, 
            strm.VALDATE, 
            strm.VARIATION_ID, 
            strm.VERIFIED, 
            strm.XPORTALSRKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XPORTALSRKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLPORTAL_XPORTALSR as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XPORTALSRKEY=src.XPORTALSRKEY) OR (target.XPORTALSRKEY IS NULL AND src.XPORTALSRKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCTNO=src.ACCTNO, 
                    target.ACCTNOTADD=src.ACCTNOTADD, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDR1=src.ADDR1, 
                    target.ADDR2=src.ADDR2, 
                    target.CITY=src.CITY, 
                    target.COUNTRY=src.COUNTRY, 
                    target.DACCTNAME=src.DACCTNAME, 
                    target.DATEFIXD=src.DATEFIXD, 
                    target.DAYPHN=src.DAYPHN, 
                    target.DDACCTNO=src.DDACCTNO, 
                    target.DDACTYPE=src.DDACTYPE, 
                    target.DDBANK=src.DDBANK, 
                    target.DDBRANCH=src.DDBRANCH, 
                    target.DDMASKNO=src.DDMASKNO, 
                    target.DDMAXAMT=src.DDMAXAMT, 
                    target.DELETED=src.DELETED, 
                    target.EMAIL=src.EMAIL, 
                    target.EVEPHN=src.EVEPHN, 
                    target.FEDBKTYE=src.FEDBKTYE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NOOCC=src.NOOCC, 
                    target.POSTCODE=src.POSTCODE, 
                    target.QUESTTOPC=src.QUESTTOPC, 
                    target.SDLDATE=src.SDLDATE, 
                    target.SERVNO=src.SERVNO, 
                    target.STATE=src.STATE, 
                    target.USERID=src.USERID, 
                    target.VALDATE=src.VALDATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VERIFIED=src.VERIFIED, 
                    target.XPORTALSRKEY=src.XPORTALSRKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCTNO, 
                    ACCTNOTADD, 
                    ADDBY, 
                    ADDDTTM, 
                    ADDR1, 
                    ADDR2, 
                    CITY, 
                    COUNTRY, 
                    DACCTNAME, 
                    DATEFIXD, 
                    DAYPHN, 
                    DDACCTNO, 
                    DDACTYPE, 
                    DDBANK, 
                    DDBRANCH, 
                    DDMASKNO, 
                    DDMAXAMT, 
                    DELETED, 
                    EMAIL, 
                    EVEPHN, 
                    FEDBKTYE, 
                    MODBY, 
                    MODDTTM, 
                    NOOCC, 
                    POSTCODE, 
                    QUESTTOPC, 
                    SDLDATE, 
                    SERVNO, 
                    STATE, 
                    USERID, 
                    VALDATE, 
                    VARIATION_ID, 
                    VERIFIED, 
                    XPORTALSRKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCTNO, 
                    src.ACCTNOTADD, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDR1, 
                    src.ADDR2, 
                    src.CITY, 
                    src.COUNTRY, 
                    src.DACCTNAME, 
                    src.DATEFIXD, 
                    src.DAYPHN, 
                    src.DDACCTNO, 
                    src.DDACTYPE, 
                    src.DDBANK, 
                    src.DDBRANCH, 
                    src.DDMASKNO, 
                    src.DDMAXAMT, 
                    src.DELETED, 
                    src.EMAIL, 
                    src.EVEPHN, 
                    src.FEDBKTYE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NOOCC, 
                    src.POSTCODE, 
                    src.QUESTTOPC, 
                    src.SDLDATE, 
                    src.SERVNO, 
                    src.STATE, 
                    src.USERID, 
                    src.VALDATE, 
                    src.VARIATION_ID, 
                    src.VERIFIED, 
                    src.XPORTALSRKEY,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLPORTAL_XPORTALSR as target using (
                SELECT * FROM (SELECT 
            strm.ACCTNO, 
            strm.ACCTNOTADD, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDR1, 
            strm.ADDR2, 
            strm.CITY, 
            strm.COUNTRY, 
            strm.DACCTNAME, 
            strm.DATEFIXD, 
            strm.DAYPHN, 
            strm.DDACCTNO, 
            strm.DDACTYPE, 
            strm.DDBANK, 
            strm.DDBRANCH, 
            strm.DDMASKNO, 
            strm.DDMAXAMT, 
            strm.DELETED, 
            strm.EMAIL, 
            strm.EVEPHN, 
            strm.FEDBKTYE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NOOCC, 
            strm.POSTCODE, 
            strm.QUESTTOPC, 
            strm.SDLDATE, 
            strm.SERVNO, 
            strm.STATE, 
            strm.USERID, 
            strm.VALDATE, 
            strm.VARIATION_ID, 
            strm.VERIFIED, 
            strm.XPORTALSRKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XPORTALSRKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLPORTAL_XPORTALSR as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XPORTALSRKEY=src.XPORTALSRKEY) OR (target.XPORTALSRKEY IS NULL AND src.XPORTALSRKEY IS NULL)) 
                when matched then update set
                    target.ACCTNO=src.ACCTNO, 
                    target.ACCTNOTADD=src.ACCTNOTADD, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDR1=src.ADDR1, 
                    target.ADDR2=src.ADDR2, 
                    target.CITY=src.CITY, 
                    target.COUNTRY=src.COUNTRY, 
                    target.DACCTNAME=src.DACCTNAME, 
                    target.DATEFIXD=src.DATEFIXD, 
                    target.DAYPHN=src.DAYPHN, 
                    target.DDACCTNO=src.DDACCTNO, 
                    target.DDACTYPE=src.DDACTYPE, 
                    target.DDBANK=src.DDBANK, 
                    target.DDBRANCH=src.DDBRANCH, 
                    target.DDMASKNO=src.DDMASKNO, 
                    target.DDMAXAMT=src.DDMAXAMT, 
                    target.DELETED=src.DELETED, 
                    target.EMAIL=src.EMAIL, 
                    target.EVEPHN=src.EVEPHN, 
                    target.FEDBKTYE=src.FEDBKTYE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NOOCC=src.NOOCC, 
                    target.POSTCODE=src.POSTCODE, 
                    target.QUESTTOPC=src.QUESTTOPC, 
                    target.SDLDATE=src.SDLDATE, 
                    target.SERVNO=src.SERVNO, 
                    target.STATE=src.STATE, 
                    target.USERID=src.USERID, 
                    target.VALDATE=src.VALDATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VERIFIED=src.VERIFIED, 
                    target.XPORTALSRKEY=src.XPORTALSRKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCTNO, ACCTNOTADD, ADDBY, ADDDTTM, ADDR1, ADDR2, CITY, COUNTRY, DACCTNAME, DATEFIXD, DAYPHN, DDACCTNO, DDACTYPE, DDBANK, DDBRANCH, DDMASKNO, DDMAXAMT, DELETED, EMAIL, EVEPHN, FEDBKTYE, MODBY, MODDTTM, NOOCC, POSTCODE, QUESTTOPC, SDLDATE, SERVNO, STATE, USERID, VALDATE, VARIATION_ID, VERIFIED, XPORTALSRKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCTNO, 
                    src.ACCTNOTADD, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDR1, 
                    src.ADDR2, 
                    src.CITY, 
                    src.COUNTRY, 
                    src.DACCTNAME, 
                    src.DATEFIXD, 
                    src.DAYPHN, 
                    src.DDACCTNO, 
                    src.DDACTYPE, 
                    src.DDBANK, 
                    src.DDBRANCH, 
                    src.DDMASKNO, 
                    src.DDMAXAMT, 
                    src.DELETED, 
                    src.EMAIL, 
                    src.EVEPHN, 
                    src.FEDBKTYE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NOOCC, 
                    src.POSTCODE, 
                    src.QUESTTOPC, 
                    src.SDLDATE, 
                    src.SERVNO, 
                    src.STATE, 
                    src.USERID, 
                    src.VALDATE, 
                    src.VARIATION_ID, 
                    src.VERIFIED, 
                    src.XPORTALSRKEY,     
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