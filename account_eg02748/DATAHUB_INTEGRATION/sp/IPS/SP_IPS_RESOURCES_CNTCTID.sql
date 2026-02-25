create or replace procedure DATAHUB_INTEGRATION.SP_IPS_RESOURCES_CNTCTID()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_RESOURCES_CNTCTID_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_RESOURCES_CNTCTID as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.CONFIDENTFLAG, 
            strm.CONTRACTORID, 
            strm.CONTRACTORRATE, 
            strm.CONTRACTORTYPE, 
            strm.CUSTOMERNO, 
            strm.DEATHDATE, 
            strm.DELETED, 
            strm.DOB, 
            strm.DRIVERLICENSENO, 
            strm.DRIVERLICENSESTATE, 
            strm.EXPDATE, 
            strm.EXTID, 
            strm.FEDTAXID, 
            strm.FULLNAME, 
            strm.HINT, 
            strm.IDKEY, 
            strm.IDLAST4, 
            strm.IDNO, 
            strm.IDTYPE, 
            strm.ISCONTRACTOR, 
            strm.LANGUAGE, 
            strm.MAIDENNAME, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NAMEFIRST, 
            strm.NAMELAST, 
            strm.NAMEMID, 
            strm.PASSWORD, 
            strm.PIN, 
            strm.PRIMARYDUPLICATE, 
            strm.REQUESTEDNAME, 
            strm.SECURITYANSWER, 
            strm.STTAXID, 
            strm.SUFFIX, 
            strm.TITLE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.IDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_CNTCTID as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.IDKEY=src.IDKEY) OR (target.IDKEY IS NULL AND src.IDKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.CONFIDENTFLAG=src.CONFIDENTFLAG, 
                    target.CONTRACTORID=src.CONTRACTORID, 
                    target.CONTRACTORRATE=src.CONTRACTORRATE, 
                    target.CONTRACTORTYPE=src.CONTRACTORTYPE, 
                    target.CUSTOMERNO=src.CUSTOMERNO, 
                    target.DEATHDATE=src.DEATHDATE, 
                    target.DELETED=src.DELETED, 
                    target.DOB=src.DOB, 
                    target.DRIVERLICENSENO=src.DRIVERLICENSENO, 
                    target.DRIVERLICENSESTATE=src.DRIVERLICENSESTATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.EXTID=src.EXTID, 
                    target.FEDTAXID=src.FEDTAXID, 
                    target.FULLNAME=src.FULLNAME, 
                    target.HINT=src.HINT, 
                    target.IDKEY=src.IDKEY, 
                    target.IDLAST4=src.IDLAST4, 
                    target.IDNO=src.IDNO, 
                    target.IDTYPE=src.IDTYPE, 
                    target.ISCONTRACTOR=src.ISCONTRACTOR, 
                    target.LANGUAGE=src.LANGUAGE, 
                    target.MAIDENNAME=src.MAIDENNAME, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NAMEFIRST=src.NAMEFIRST, 
                    target.NAMELAST=src.NAMELAST, 
                    target.NAMEMID=src.NAMEMID, 
                    target.PASSWORD=src.PASSWORD, 
                    target.PIN=src.PIN, 
                    target.PRIMARYDUPLICATE=src.PRIMARYDUPLICATE, 
                    target.REQUESTEDNAME=src.REQUESTEDNAME, 
                    target.SECURITYANSWER=src.SECURITYANSWER, 
                    target.STTAXID=src.STTAXID, 
                    target.SUFFIX=src.SUFFIX, 
                    target.TITLE=src.TITLE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    CONFIDENTFLAG, 
                    CONTRACTORID, 
                    CONTRACTORRATE, 
                    CONTRACTORTYPE, 
                    CUSTOMERNO, 
                    DEATHDATE, 
                    DELETED, 
                    DOB, 
                    DRIVERLICENSENO, 
                    DRIVERLICENSESTATE, 
                    EXPDATE, 
                    EXTID, 
                    FEDTAXID, 
                    FULLNAME, 
                    HINT, 
                    IDKEY, 
                    IDLAST4, 
                    IDNO, 
                    IDTYPE, 
                    ISCONTRACTOR, 
                    LANGUAGE, 
                    MAIDENNAME, 
                    MODBY, 
                    MODDTTM, 
                    NAMEFIRST, 
                    NAMELAST, 
                    NAMEMID, 
                    PASSWORD, 
                    PIN, 
                    PRIMARYDUPLICATE, 
                    REQUESTEDNAME, 
                    SECURITYANSWER, 
                    STTAXID, 
                    SUFFIX, 
                    TITLE, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.CONFIDENTFLAG, 
                    src.CONTRACTORID, 
                    src.CONTRACTORRATE, 
                    src.CONTRACTORTYPE, 
                    src.CUSTOMERNO, 
                    src.DEATHDATE, 
                    src.DELETED, 
                    src.DOB, 
                    src.DRIVERLICENSENO, 
                    src.DRIVERLICENSESTATE, 
                    src.EXPDATE, 
                    src.EXTID, 
                    src.FEDTAXID, 
                    src.FULLNAME, 
                    src.HINT, 
                    src.IDKEY, 
                    src.IDLAST4, 
                    src.IDNO, 
                    src.IDTYPE, 
                    src.ISCONTRACTOR, 
                    src.LANGUAGE, 
                    src.MAIDENNAME, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NAMEFIRST, 
                    src.NAMELAST, 
                    src.NAMEMID, 
                    src.PASSWORD, 
                    src.PIN, 
                    src.PRIMARYDUPLICATE, 
                    src.REQUESTEDNAME, 
                    src.SECURITYANSWER, 
                    src.STTAXID, 
                    src.SUFFIX, 
                    src.TITLE, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_RESOURCES_CNTCTID as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.CONFIDENTFLAG, 
            strm.CONTRACTORID, 
            strm.CONTRACTORRATE, 
            strm.CONTRACTORTYPE, 
            strm.CUSTOMERNO, 
            strm.DEATHDATE, 
            strm.DELETED, 
            strm.DOB, 
            strm.DRIVERLICENSENO, 
            strm.DRIVERLICENSESTATE, 
            strm.EXPDATE, 
            strm.EXTID, 
            strm.FEDTAXID, 
            strm.FULLNAME, 
            strm.HINT, 
            strm.IDKEY, 
            strm.IDLAST4, 
            strm.IDNO, 
            strm.IDTYPE, 
            strm.ISCONTRACTOR, 
            strm.LANGUAGE, 
            strm.MAIDENNAME, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NAMEFIRST, 
            strm.NAMELAST, 
            strm.NAMEMID, 
            strm.PASSWORD, 
            strm.PIN, 
            strm.PRIMARYDUPLICATE, 
            strm.REQUESTEDNAME, 
            strm.SECURITYANSWER, 
            strm.STTAXID, 
            strm.SUFFIX, 
            strm.TITLE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.IDKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_RESOURCES_CNTCTID as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.IDKEY=src.IDKEY) OR (target.IDKEY IS NULL AND src.IDKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.CONFIDENTFLAG=src.CONFIDENTFLAG, 
                    target.CONTRACTORID=src.CONTRACTORID, 
                    target.CONTRACTORRATE=src.CONTRACTORRATE, 
                    target.CONTRACTORTYPE=src.CONTRACTORTYPE, 
                    target.CUSTOMERNO=src.CUSTOMERNO, 
                    target.DEATHDATE=src.DEATHDATE, 
                    target.DELETED=src.DELETED, 
                    target.DOB=src.DOB, 
                    target.DRIVERLICENSENO=src.DRIVERLICENSENO, 
                    target.DRIVERLICENSESTATE=src.DRIVERLICENSESTATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.EXTID=src.EXTID, 
                    target.FEDTAXID=src.FEDTAXID, 
                    target.FULLNAME=src.FULLNAME, 
                    target.HINT=src.HINT, 
                    target.IDKEY=src.IDKEY, 
                    target.IDLAST4=src.IDLAST4, 
                    target.IDNO=src.IDNO, 
                    target.IDTYPE=src.IDTYPE, 
                    target.ISCONTRACTOR=src.ISCONTRACTOR, 
                    target.LANGUAGE=src.LANGUAGE, 
                    target.MAIDENNAME=src.MAIDENNAME, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NAMEFIRST=src.NAMEFIRST, 
                    target.NAMELAST=src.NAMELAST, 
                    target.NAMEMID=src.NAMEMID, 
                    target.PASSWORD=src.PASSWORD, 
                    target.PIN=src.PIN, 
                    target.PRIMARYDUPLICATE=src.PRIMARYDUPLICATE, 
                    target.REQUESTEDNAME=src.REQUESTEDNAME, 
                    target.SECURITYANSWER=src.SECURITYANSWER, 
                    target.STTAXID=src.STTAXID, 
                    target.SUFFIX=src.SUFFIX, 
                    target.TITLE=src.TITLE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, CONFIDENTFLAG, CONTRACTORID, CONTRACTORRATE, CONTRACTORTYPE, CUSTOMERNO, DEATHDATE, DELETED, DOB, DRIVERLICENSENO, DRIVERLICENSESTATE, EXPDATE, EXTID, FEDTAXID, FULLNAME, HINT, IDKEY, IDLAST4, IDNO, IDTYPE, ISCONTRACTOR, LANGUAGE, MAIDENNAME, MODBY, MODDTTM, NAMEFIRST, NAMELAST, NAMEMID, PASSWORD, PIN, PRIMARYDUPLICATE, REQUESTEDNAME, SECURITYANSWER, STTAXID, SUFFIX, TITLE, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.CONFIDENTFLAG, 
                    src.CONTRACTORID, 
                    src.CONTRACTORRATE, 
                    src.CONTRACTORTYPE, 
                    src.CUSTOMERNO, 
                    src.DEATHDATE, 
                    src.DELETED, 
                    src.DOB, 
                    src.DRIVERLICENSENO, 
                    src.DRIVERLICENSESTATE, 
                    src.EXPDATE, 
                    src.EXTID, 
                    src.FEDTAXID, 
                    src.FULLNAME, 
                    src.HINT, 
                    src.IDKEY, 
                    src.IDLAST4, 
                    src.IDNO, 
                    src.IDTYPE, 
                    src.ISCONTRACTOR, 
                    src.LANGUAGE, 
                    src.MAIDENNAME, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NAMEFIRST, 
                    src.NAMELAST, 
                    src.NAMEMID, 
                    src.PASSWORD, 
                    src.PIN, 
                    src.PRIMARYDUPLICATE, 
                    src.REQUESTEDNAME, 
                    src.SECURITYANSWER, 
                    src.STTAXID, 
                    src.SUFFIX, 
                    src.TITLE, 
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