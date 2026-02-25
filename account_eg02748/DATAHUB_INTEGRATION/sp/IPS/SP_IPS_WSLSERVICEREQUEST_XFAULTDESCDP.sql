create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLSERVICEREQUEST_XFAULTDESCDP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLSERVICEREQUEST_XFAULTDESCDP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLSERVICEREQUEST_XFAULTDESCDP as target using (SELECT * FROM (SELECT 
            strm.ACCESSISSUES, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ASSETID, 
            strm.BOTTLEDWATER, 
            strm.CCTV, 
            strm.CONTRACTORAREA, 
            strm.CONTRACTORCODE, 
            strm.CONTRACTORREFNO, 
            strm.DELETED, 
            strm.DIALYSISPATIENT, 
            strm.DOCTORCALLED, 
            strm.FAULTLOCATION, 
            strm.FAULTTYPE, 
            strm.FLUSH, 
            strm.FREQUENCY, 
            strm.GATEVALVE, 
            strm.HEALTHSAFETY, 
            strm.INTERNALDRAINAGE, 
            strm.JOBCBD, 
            strm.KEYACCOUNT, 
            strm.METERASSETID, 
            strm.METERASSETLOCATION, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MULTIPLEPROPERTIES, 
            strm.NEIGHBOURSAFFECTED, 
            strm.NUMBERSRLAST, 
            strm.PHOTOSPROVIDED, 
            strm.PLANNEDMETER, 
            strm.PLUMBERCONTACTED, 
            strm.PLUMBINGWORK, 
            strm.PROJECTRELATED, 
            strm.PROPERTYDAMAGE, 
            strm.PUBLICHEALTHRISK, 
            strm.RAINWATERTANK, 
            strm.RELATEDSR, 
            strm.RESIDENTFEELING, 
            strm.RESIDENTILL, 
            strm.ROADWATERMAINBREAK, 
            strm.SCHEDULEDSITETREE, 
            strm.SERVICEAREA, 
            strm.SERVNO, 
            strm.SIZEOFLEAK, 
            strm.SUPERVISOR, 
            strm.SYMPTOMSSTARTED, 
            strm.TAPS, 
            strm.THIRDPARTY, 
            strm.TREE, 
            strm.VARIATION_ID, 
            strm.WTRCOLORSMELLPART, 
            strm.XFAULTDESCDPKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XFAULTDESCDPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLSERVICEREQUEST_XFAULTDESCDP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XFAULTDESCDPKEY=src.XFAULTDESCDPKEY) OR (target.XFAULTDESCDPKEY IS NULL AND src.XFAULTDESCDPKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCESSISSUES=src.ACCESSISSUES, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ASSETID=src.ASSETID, 
                    target.BOTTLEDWATER=src.BOTTLEDWATER, 
                    target.CCTV=src.CCTV, 
                    target.CONTRACTORAREA=src.CONTRACTORAREA, 
                    target.CONTRACTORCODE=src.CONTRACTORCODE, 
                    target.CONTRACTORREFNO=src.CONTRACTORREFNO, 
                    target.DELETED=src.DELETED, 
                    target.DIALYSISPATIENT=src.DIALYSISPATIENT, 
                    target.DOCTORCALLED=src.DOCTORCALLED, 
                    target.FAULTLOCATION=src.FAULTLOCATION, 
                    target.FAULTTYPE=src.FAULTTYPE, 
                    target.FLUSH=src.FLUSH, 
                    target.FREQUENCY=src.FREQUENCY, 
                    target.GATEVALVE=src.GATEVALVE, 
                    target.HEALTHSAFETY=src.HEALTHSAFETY, 
                    target.INTERNALDRAINAGE=src.INTERNALDRAINAGE, 
                    target.JOBCBD=src.JOBCBD, 
                    target.KEYACCOUNT=src.KEYACCOUNT, 
                    target.METERASSETID=src.METERASSETID, 
                    target.METERASSETLOCATION=src.METERASSETLOCATION, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MULTIPLEPROPERTIES=src.MULTIPLEPROPERTIES, 
                    target.NEIGHBOURSAFFECTED=src.NEIGHBOURSAFFECTED, 
                    target.NUMBERSRLAST=src.NUMBERSRLAST, 
                    target.PHOTOSPROVIDED=src.PHOTOSPROVIDED, 
                    target.PLANNEDMETER=src.PLANNEDMETER, 
                    target.PLUMBERCONTACTED=src.PLUMBERCONTACTED, 
                    target.PLUMBINGWORK=src.PLUMBINGWORK, 
                    target.PROJECTRELATED=src.PROJECTRELATED, 
                    target.PROPERTYDAMAGE=src.PROPERTYDAMAGE, 
                    target.PUBLICHEALTHRISK=src.PUBLICHEALTHRISK, 
                    target.RAINWATERTANK=src.RAINWATERTANK, 
                    target.RELATEDSR=src.RELATEDSR, 
                    target.RESIDENTFEELING=src.RESIDENTFEELING, 
                    target.RESIDENTILL=src.RESIDENTILL, 
                    target.ROADWATERMAINBREAK=src.ROADWATERMAINBREAK, 
                    target.SCHEDULEDSITETREE=src.SCHEDULEDSITETREE, 
                    target.SERVICEAREA=src.SERVICEAREA, 
                    target.SERVNO=src.SERVNO, 
                    target.SIZEOFLEAK=src.SIZEOFLEAK, 
                    target.SUPERVISOR=src.SUPERVISOR, 
                    target.SYMPTOMSSTARTED=src.SYMPTOMSSTARTED, 
                    target.TAPS=src.TAPS, 
                    target.THIRDPARTY=src.THIRDPARTY, 
                    target.TREE=src.TREE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WTRCOLORSMELLPART=src.WTRCOLORSMELLPART, 
                    target.XFAULTDESCDPKEY=src.XFAULTDESCDPKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCESSISSUES, 
                    ADDBY, 
                    ADDDTTM, 
                    ASSETID, 
                    BOTTLEDWATER, 
                    CCTV, 
                    CONTRACTORAREA, 
                    CONTRACTORCODE, 
                    CONTRACTORREFNO, 
                    DELETED, 
                    DIALYSISPATIENT, 
                    DOCTORCALLED, 
                    FAULTLOCATION, 
                    FAULTTYPE, 
                    FLUSH, 
                    FREQUENCY, 
                    GATEVALVE, 
                    HEALTHSAFETY, 
                    INTERNALDRAINAGE, 
                    JOBCBD, 
                    KEYACCOUNT, 
                    METERASSETID, 
                    METERASSETLOCATION, 
                    MODBY, 
                    MODDTTM, 
                    MULTIPLEPROPERTIES, 
                    NEIGHBOURSAFFECTED, 
                    NUMBERSRLAST, 
                    PHOTOSPROVIDED, 
                    PLANNEDMETER, 
                    PLUMBERCONTACTED, 
                    PLUMBINGWORK, 
                    PROJECTRELATED, 
                    PROPERTYDAMAGE, 
                    PUBLICHEALTHRISK, 
                    RAINWATERTANK, 
                    RELATEDSR, 
                    RESIDENTFEELING, 
                    RESIDENTILL, 
                    ROADWATERMAINBREAK, 
                    SCHEDULEDSITETREE, 
                    SERVICEAREA, 
                    SERVNO, 
                    SIZEOFLEAK, 
                    SUPERVISOR, 
                    SYMPTOMSSTARTED, 
                    TAPS, 
                    THIRDPARTY, 
                    TREE, 
                    VARIATION_ID, 
                    WTRCOLORSMELLPART, 
                    XFAULTDESCDPKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCESSISSUES, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ASSETID, 
                    src.BOTTLEDWATER, 
                    src.CCTV, 
                    src.CONTRACTORAREA, 
                    src.CONTRACTORCODE, 
                    src.CONTRACTORREFNO, 
                    src.DELETED, 
                    src.DIALYSISPATIENT, 
                    src.DOCTORCALLED, 
                    src.FAULTLOCATION, 
                    src.FAULTTYPE, 
                    src.FLUSH, 
                    src.FREQUENCY, 
                    src.GATEVALVE, 
                    src.HEALTHSAFETY, 
                    src.INTERNALDRAINAGE, 
                    src.JOBCBD, 
                    src.KEYACCOUNT, 
                    src.METERASSETID, 
                    src.METERASSETLOCATION, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MULTIPLEPROPERTIES, 
                    src.NEIGHBOURSAFFECTED, 
                    src.NUMBERSRLAST, 
                    src.PHOTOSPROVIDED, 
                    src.PLANNEDMETER, 
                    src.PLUMBERCONTACTED, 
                    src.PLUMBINGWORK, 
                    src.PROJECTRELATED, 
                    src.PROPERTYDAMAGE, 
                    src.PUBLICHEALTHRISK, 
                    src.RAINWATERTANK, 
                    src.RELATEDSR, 
                    src.RESIDENTFEELING, 
                    src.RESIDENTILL, 
                    src.ROADWATERMAINBREAK, 
                    src.SCHEDULEDSITETREE, 
                    src.SERVICEAREA, 
                    src.SERVNO, 
                    src.SIZEOFLEAK, 
                    src.SUPERVISOR, 
                    src.SYMPTOMSSTARTED, 
                    src.TAPS, 
                    src.THIRDPARTY, 
                    src.TREE, 
                    src.VARIATION_ID, 
                    src.WTRCOLORSMELLPART, 
                    src.XFAULTDESCDPKEY,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLSERVICEREQUEST_XFAULTDESCDP as target using (
                SELECT * FROM (SELECT 
            strm.ACCESSISSUES, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ASSETID, 
            strm.BOTTLEDWATER, 
            strm.CCTV, 
            strm.CONTRACTORAREA, 
            strm.CONTRACTORCODE, 
            strm.CONTRACTORREFNO, 
            strm.DELETED, 
            strm.DIALYSISPATIENT, 
            strm.DOCTORCALLED, 
            strm.FAULTLOCATION, 
            strm.FAULTTYPE, 
            strm.FLUSH, 
            strm.FREQUENCY, 
            strm.GATEVALVE, 
            strm.HEALTHSAFETY, 
            strm.INTERNALDRAINAGE, 
            strm.JOBCBD, 
            strm.KEYACCOUNT, 
            strm.METERASSETID, 
            strm.METERASSETLOCATION, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MULTIPLEPROPERTIES, 
            strm.NEIGHBOURSAFFECTED, 
            strm.NUMBERSRLAST, 
            strm.PHOTOSPROVIDED, 
            strm.PLANNEDMETER, 
            strm.PLUMBERCONTACTED, 
            strm.PLUMBINGWORK, 
            strm.PROJECTRELATED, 
            strm.PROPERTYDAMAGE, 
            strm.PUBLICHEALTHRISK, 
            strm.RAINWATERTANK, 
            strm.RELATEDSR, 
            strm.RESIDENTFEELING, 
            strm.RESIDENTILL, 
            strm.ROADWATERMAINBREAK, 
            strm.SCHEDULEDSITETREE, 
            strm.SERVICEAREA, 
            strm.SERVNO, 
            strm.SIZEOFLEAK, 
            strm.SUPERVISOR, 
            strm.SYMPTOMSSTARTED, 
            strm.TAPS, 
            strm.THIRDPARTY, 
            strm.TREE, 
            strm.VARIATION_ID, 
            strm.WTRCOLORSMELLPART, 
            strm.XFAULTDESCDPKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XFAULTDESCDPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLSERVICEREQUEST_XFAULTDESCDP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XFAULTDESCDPKEY=src.XFAULTDESCDPKEY) OR (target.XFAULTDESCDPKEY IS NULL AND src.XFAULTDESCDPKEY IS NULL)) 
                when matched then update set
                    target.ACCESSISSUES=src.ACCESSISSUES, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ASSETID=src.ASSETID, 
                    target.BOTTLEDWATER=src.BOTTLEDWATER, 
                    target.CCTV=src.CCTV, 
                    target.CONTRACTORAREA=src.CONTRACTORAREA, 
                    target.CONTRACTORCODE=src.CONTRACTORCODE, 
                    target.CONTRACTORREFNO=src.CONTRACTORREFNO, 
                    target.DELETED=src.DELETED, 
                    target.DIALYSISPATIENT=src.DIALYSISPATIENT, 
                    target.DOCTORCALLED=src.DOCTORCALLED, 
                    target.FAULTLOCATION=src.FAULTLOCATION, 
                    target.FAULTTYPE=src.FAULTTYPE, 
                    target.FLUSH=src.FLUSH, 
                    target.FREQUENCY=src.FREQUENCY, 
                    target.GATEVALVE=src.GATEVALVE, 
                    target.HEALTHSAFETY=src.HEALTHSAFETY, 
                    target.INTERNALDRAINAGE=src.INTERNALDRAINAGE, 
                    target.JOBCBD=src.JOBCBD, 
                    target.KEYACCOUNT=src.KEYACCOUNT, 
                    target.METERASSETID=src.METERASSETID, 
                    target.METERASSETLOCATION=src.METERASSETLOCATION, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MULTIPLEPROPERTIES=src.MULTIPLEPROPERTIES, 
                    target.NEIGHBOURSAFFECTED=src.NEIGHBOURSAFFECTED, 
                    target.NUMBERSRLAST=src.NUMBERSRLAST, 
                    target.PHOTOSPROVIDED=src.PHOTOSPROVIDED, 
                    target.PLANNEDMETER=src.PLANNEDMETER, 
                    target.PLUMBERCONTACTED=src.PLUMBERCONTACTED, 
                    target.PLUMBINGWORK=src.PLUMBINGWORK, 
                    target.PROJECTRELATED=src.PROJECTRELATED, 
                    target.PROPERTYDAMAGE=src.PROPERTYDAMAGE, 
                    target.PUBLICHEALTHRISK=src.PUBLICHEALTHRISK, 
                    target.RAINWATERTANK=src.RAINWATERTANK, 
                    target.RELATEDSR=src.RELATEDSR, 
                    target.RESIDENTFEELING=src.RESIDENTFEELING, 
                    target.RESIDENTILL=src.RESIDENTILL, 
                    target.ROADWATERMAINBREAK=src.ROADWATERMAINBREAK, 
                    target.SCHEDULEDSITETREE=src.SCHEDULEDSITETREE, 
                    target.SERVICEAREA=src.SERVICEAREA, 
                    target.SERVNO=src.SERVNO, 
                    target.SIZEOFLEAK=src.SIZEOFLEAK, 
                    target.SUPERVISOR=src.SUPERVISOR, 
                    target.SYMPTOMSSTARTED=src.SYMPTOMSSTARTED, 
                    target.TAPS=src.TAPS, 
                    target.THIRDPARTY=src.THIRDPARTY, 
                    target.TREE=src.TREE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WTRCOLORSMELLPART=src.WTRCOLORSMELLPART, 
                    target.XFAULTDESCDPKEY=src.XFAULTDESCDPKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCESSISSUES, ADDBY, ADDDTTM, ASSETID, BOTTLEDWATER, CCTV, CONTRACTORAREA, CONTRACTORCODE, CONTRACTORREFNO, DELETED, DIALYSISPATIENT, DOCTORCALLED, FAULTLOCATION, FAULTTYPE, FLUSH, FREQUENCY, GATEVALVE, HEALTHSAFETY, INTERNALDRAINAGE, JOBCBD, KEYACCOUNT, METERASSETID, METERASSETLOCATION, MODBY, MODDTTM, MULTIPLEPROPERTIES, NEIGHBOURSAFFECTED, NUMBERSRLAST, PHOTOSPROVIDED, PLANNEDMETER, PLUMBERCONTACTED, PLUMBINGWORK, PROJECTRELATED, PROPERTYDAMAGE, PUBLICHEALTHRISK, RAINWATERTANK, RELATEDSR, RESIDENTFEELING, RESIDENTILL, ROADWATERMAINBREAK, SCHEDULEDSITETREE, SERVICEAREA, SERVNO, SIZEOFLEAK, SUPERVISOR, SYMPTOMSSTARTED, TAPS, THIRDPARTY, TREE, VARIATION_ID, WTRCOLORSMELLPART, XFAULTDESCDPKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCESSISSUES, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ASSETID, 
                    src.BOTTLEDWATER, 
                    src.CCTV, 
                    src.CONTRACTORAREA, 
                    src.CONTRACTORCODE, 
                    src.CONTRACTORREFNO, 
                    src.DELETED, 
                    src.DIALYSISPATIENT, 
                    src.DOCTORCALLED, 
                    src.FAULTLOCATION, 
                    src.FAULTTYPE, 
                    src.FLUSH, 
                    src.FREQUENCY, 
                    src.GATEVALVE, 
                    src.HEALTHSAFETY, 
                    src.INTERNALDRAINAGE, 
                    src.JOBCBD, 
                    src.KEYACCOUNT, 
                    src.METERASSETID, 
                    src.METERASSETLOCATION, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MULTIPLEPROPERTIES, 
                    src.NEIGHBOURSAFFECTED, 
                    src.NUMBERSRLAST, 
                    src.PHOTOSPROVIDED, 
                    src.PLANNEDMETER, 
                    src.PLUMBERCONTACTED, 
                    src.PLUMBINGWORK, 
                    src.PROJECTRELATED, 
                    src.PROPERTYDAMAGE, 
                    src.PUBLICHEALTHRISK, 
                    src.RAINWATERTANK, 
                    src.RELATEDSR, 
                    src.RESIDENTFEELING, 
                    src.RESIDENTILL, 
                    src.ROADWATERMAINBREAK, 
                    src.SCHEDULEDSITETREE, 
                    src.SERVICEAREA, 
                    src.SERVNO, 
                    src.SIZEOFLEAK, 
                    src.SUPERVISOR, 
                    src.SYMPTOMSSTARTED, 
                    src.TAPS, 
                    src.THIRDPARTY, 
                    src.TREE, 
                    src.VARIATION_ID, 
                    src.WTRCOLORSMELLPART, 
                    src.XFAULTDESCDPKEY,     
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