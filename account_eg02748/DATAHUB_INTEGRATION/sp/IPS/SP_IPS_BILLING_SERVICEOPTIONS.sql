create or replace procedure DATAHUB_INTEGRATION.SP_IPS_BILLING_SERVICEOPTIONS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_BILLING_SERVICEOPTIONS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_BILLING_SERVICEOPTIONS as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ASSETTYPE, 
            strm.AVAILABILITY, 
            strm.BUILDERFLAG, 
            strm.CLASS, 
            strm.CODE, 
            strm.CONSGRAPHFLAG, 
            strm.CONSUMPTIONPERCENTAGE, 
            strm.DELETED, 
            strm.DONOTPRORATEMOVEINFLAG, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.FIXEDCHGINADVANCEFLAG, 
            strm.FREQUENCY, 
            strm.IMPERVIOUSAREAONLYFLAG, 
            strm.LEARNMOREKBKEY, 
            strm.METERTYPEFORMULA, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MOVEINACTKEY, 
            strm.MOVEINCOLOR, 
            strm.MOVEINNOTES, 
            strm.MOVEINREQSKEY, 
            strm.MOVEINSRWOCONDITIONFORMULA, 
            strm.MOVEINSWOKEY, 
            strm.MOVEOUTACTKEY, 
            strm.MOVEOUTLEARNMOREKBKEY, 
            strm.MOVEOUTNOTES, 
            strm.MOVEOUTREQSKEY, 
            strm.MOVEOUTSRWOCONDITIONFORMULA, 
            strm.MOVEOUTSWOKEY, 
            strm.OUTPUTHISTORICALUSAGEFLAG, 
            strm.OWNERFLAG, 
            strm.PRORATESERVICESTARTFLAG, 
            strm.PRORATESERVICESTOPFLAG, 
            strm.SERVICECLASS1, 
            strm.SERVICECLASS2, 
            strm.SERVICEDEFINITIONKEY, 
            strm.SERVICEDISTRICT, 
            strm.SERVICEFIELD1KEY, 
            strm.SERVICEFIELD2KEY, 
            strm.SERVICEFIELD3KEY, 
            strm.SERVICEOPTIONSKEY, 
            strm.SERVICETYPE, 
            strm.TENANTFLAG, 
            strm.TRADEWASTEFLAG, 
            strm.TRADEWASTEMAX, 
            strm.TRADEWASTERATE, 
            strm.VARIATION_ID, 
            strm.WINTERAVGFLAG, 
            strm.WINTERAVGSETUPKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.SERVICEOPTIONSKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_SERVICEOPTIONS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.SERVICEOPTIONSKEY=src.SERVICEOPTIONSKEY) OR (target.SERVICEOPTIONSKEY IS NULL AND src.SERVICEOPTIONSKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ASSETTYPE=src.ASSETTYPE, 
                    target.AVAILABILITY=src.AVAILABILITY, 
                    target.BUILDERFLAG=src.BUILDERFLAG, 
                    target.CLASS=src.CLASS, 
                    target.CODE=src.CODE, 
                    target.CONSGRAPHFLAG=src.CONSGRAPHFLAG, 
                    target.CONSUMPTIONPERCENTAGE=src.CONSUMPTIONPERCENTAGE, 
                    target.DELETED=src.DELETED, 
                    target.DONOTPRORATEMOVEINFLAG=src.DONOTPRORATEMOVEINFLAG, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.FIXEDCHGINADVANCEFLAG=src.FIXEDCHGINADVANCEFLAG, 
                    target.FREQUENCY=src.FREQUENCY, 
                    target.IMPERVIOUSAREAONLYFLAG=src.IMPERVIOUSAREAONLYFLAG, 
                    target.LEARNMOREKBKEY=src.LEARNMOREKBKEY, 
                    target.METERTYPEFORMULA=src.METERTYPEFORMULA, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MOVEINACTKEY=src.MOVEINACTKEY, 
                    target.MOVEINCOLOR=src.MOVEINCOLOR, 
                    target.MOVEINNOTES=src.MOVEINNOTES, 
                    target.MOVEINREQSKEY=src.MOVEINREQSKEY, 
                    target.MOVEINSRWOCONDITIONFORMULA=src.MOVEINSRWOCONDITIONFORMULA, 
                    target.MOVEINSWOKEY=src.MOVEINSWOKEY, 
                    target.MOVEOUTACTKEY=src.MOVEOUTACTKEY, 
                    target.MOVEOUTLEARNMOREKBKEY=src.MOVEOUTLEARNMOREKBKEY, 
                    target.MOVEOUTNOTES=src.MOVEOUTNOTES, 
                    target.MOVEOUTREQSKEY=src.MOVEOUTREQSKEY, 
                    target.MOVEOUTSRWOCONDITIONFORMULA=src.MOVEOUTSRWOCONDITIONFORMULA, 
                    target.MOVEOUTSWOKEY=src.MOVEOUTSWOKEY, 
                    target.OUTPUTHISTORICALUSAGEFLAG=src.OUTPUTHISTORICALUSAGEFLAG, 
                    target.OWNERFLAG=src.OWNERFLAG, 
                    target.PRORATESERVICESTARTFLAG=src.PRORATESERVICESTARTFLAG, 
                    target.PRORATESERVICESTOPFLAG=src.PRORATESERVICESTOPFLAG, 
                    target.SERVICECLASS1=src.SERVICECLASS1, 
                    target.SERVICECLASS2=src.SERVICECLASS2, 
                    target.SERVICEDEFINITIONKEY=src.SERVICEDEFINITIONKEY, 
                    target.SERVICEDISTRICT=src.SERVICEDISTRICT, 
                    target.SERVICEFIELD1KEY=src.SERVICEFIELD1KEY, 
                    target.SERVICEFIELD2KEY=src.SERVICEFIELD2KEY, 
                    target.SERVICEFIELD3KEY=src.SERVICEFIELD3KEY, 
                    target.SERVICEOPTIONSKEY=src.SERVICEOPTIONSKEY, 
                    target.SERVICETYPE=src.SERVICETYPE, 
                    target.TENANTFLAG=src.TENANTFLAG, 
                    target.TRADEWASTEFLAG=src.TRADEWASTEFLAG, 
                    target.TRADEWASTEMAX=src.TRADEWASTEMAX, 
                    target.TRADEWASTERATE=src.TRADEWASTERATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WINTERAVGFLAG=src.WINTERAVGFLAG, 
                    target.WINTERAVGSETUPKEY=src.WINTERAVGSETUPKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    ASSETTYPE, 
                    AVAILABILITY, 
                    BUILDERFLAG, 
                    CLASS, 
                    CODE, 
                    CONSGRAPHFLAG, 
                    CONSUMPTIONPERCENTAGE, 
                    DELETED, 
                    DONOTPRORATEMOVEINFLAG, 
                    EFFDATE, 
                    EXPDATE, 
                    FIXEDCHGINADVANCEFLAG, 
                    FREQUENCY, 
                    IMPERVIOUSAREAONLYFLAG, 
                    LEARNMOREKBKEY, 
                    METERTYPEFORMULA, 
                    MODBY, 
                    MODDTTM, 
                    MOVEINACTKEY, 
                    MOVEINCOLOR, 
                    MOVEINNOTES, 
                    MOVEINREQSKEY, 
                    MOVEINSRWOCONDITIONFORMULA, 
                    MOVEINSWOKEY, 
                    MOVEOUTACTKEY, 
                    MOVEOUTLEARNMOREKBKEY, 
                    MOVEOUTNOTES, 
                    MOVEOUTREQSKEY, 
                    MOVEOUTSRWOCONDITIONFORMULA, 
                    MOVEOUTSWOKEY, 
                    OUTPUTHISTORICALUSAGEFLAG, 
                    OWNERFLAG, 
                    PRORATESERVICESTARTFLAG, 
                    PRORATESERVICESTOPFLAG, 
                    SERVICECLASS1, 
                    SERVICECLASS2, 
                    SERVICEDEFINITIONKEY, 
                    SERVICEDISTRICT, 
                    SERVICEFIELD1KEY, 
                    SERVICEFIELD2KEY, 
                    SERVICEFIELD3KEY, 
                    SERVICEOPTIONSKEY, 
                    SERVICETYPE, 
                    TENANTFLAG, 
                    TRADEWASTEFLAG, 
                    TRADEWASTEMAX, 
                    TRADEWASTERATE, 
                    VARIATION_ID, 
                    WINTERAVGFLAG, 
                    WINTERAVGSETUPKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ASSETTYPE, 
                    src.AVAILABILITY, 
                    src.BUILDERFLAG, 
                    src.CLASS, 
                    src.CODE, 
                    src.CONSGRAPHFLAG, 
                    src.CONSUMPTIONPERCENTAGE, 
                    src.DELETED, 
                    src.DONOTPRORATEMOVEINFLAG, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.FIXEDCHGINADVANCEFLAG, 
                    src.FREQUENCY, 
                    src.IMPERVIOUSAREAONLYFLAG, 
                    src.LEARNMOREKBKEY, 
                    src.METERTYPEFORMULA, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MOVEINACTKEY, 
                    src.MOVEINCOLOR, 
                    src.MOVEINNOTES, 
                    src.MOVEINREQSKEY, 
                    src.MOVEINSRWOCONDITIONFORMULA, 
                    src.MOVEINSWOKEY, 
                    src.MOVEOUTACTKEY, 
                    src.MOVEOUTLEARNMOREKBKEY, 
                    src.MOVEOUTNOTES, 
                    src.MOVEOUTREQSKEY, 
                    src.MOVEOUTSRWOCONDITIONFORMULA, 
                    src.MOVEOUTSWOKEY, 
                    src.OUTPUTHISTORICALUSAGEFLAG, 
                    src.OWNERFLAG, 
                    src.PRORATESERVICESTARTFLAG, 
                    src.PRORATESERVICESTOPFLAG, 
                    src.SERVICECLASS1, 
                    src.SERVICECLASS2, 
                    src.SERVICEDEFINITIONKEY, 
                    src.SERVICEDISTRICT, 
                    src.SERVICEFIELD1KEY, 
                    src.SERVICEFIELD2KEY, 
                    src.SERVICEFIELD3KEY, 
                    src.SERVICEOPTIONSKEY, 
                    src.SERVICETYPE, 
                    src.TENANTFLAG, 
                    src.TRADEWASTEFLAG, 
                    src.TRADEWASTEMAX, 
                    src.TRADEWASTERATE, 
                    src.VARIATION_ID, 
                    src.WINTERAVGFLAG, 
                    src.WINTERAVGSETUPKEY,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_BILLING_SERVICEOPTIONS as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ASSETTYPE, 
            strm.AVAILABILITY, 
            strm.BUILDERFLAG, 
            strm.CLASS, 
            strm.CODE, 
            strm.CONSGRAPHFLAG, 
            strm.CONSUMPTIONPERCENTAGE, 
            strm.DELETED, 
            strm.DONOTPRORATEMOVEINFLAG, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.FIXEDCHGINADVANCEFLAG, 
            strm.FREQUENCY, 
            strm.IMPERVIOUSAREAONLYFLAG, 
            strm.LEARNMOREKBKEY, 
            strm.METERTYPEFORMULA, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MOVEINACTKEY, 
            strm.MOVEINCOLOR, 
            strm.MOVEINNOTES, 
            strm.MOVEINREQSKEY, 
            strm.MOVEINSRWOCONDITIONFORMULA, 
            strm.MOVEINSWOKEY, 
            strm.MOVEOUTACTKEY, 
            strm.MOVEOUTLEARNMOREKBKEY, 
            strm.MOVEOUTNOTES, 
            strm.MOVEOUTREQSKEY, 
            strm.MOVEOUTSRWOCONDITIONFORMULA, 
            strm.MOVEOUTSWOKEY, 
            strm.OUTPUTHISTORICALUSAGEFLAG, 
            strm.OWNERFLAG, 
            strm.PRORATESERVICESTARTFLAG, 
            strm.PRORATESERVICESTOPFLAG, 
            strm.SERVICECLASS1, 
            strm.SERVICECLASS2, 
            strm.SERVICEDEFINITIONKEY, 
            strm.SERVICEDISTRICT, 
            strm.SERVICEFIELD1KEY, 
            strm.SERVICEFIELD2KEY, 
            strm.SERVICEFIELD3KEY, 
            strm.SERVICEOPTIONSKEY, 
            strm.SERVICETYPE, 
            strm.TENANTFLAG, 
            strm.TRADEWASTEFLAG, 
            strm.TRADEWASTEMAX, 
            strm.TRADEWASTERATE, 
            strm.VARIATION_ID, 
            strm.WINTERAVGFLAG, 
            strm.WINTERAVGSETUPKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.SERVICEOPTIONSKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_BILLING_SERVICEOPTIONS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.SERVICEOPTIONSKEY=src.SERVICEOPTIONSKEY) OR (target.SERVICEOPTIONSKEY IS NULL AND src.SERVICEOPTIONSKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ASSETTYPE=src.ASSETTYPE, 
                    target.AVAILABILITY=src.AVAILABILITY, 
                    target.BUILDERFLAG=src.BUILDERFLAG, 
                    target.CLASS=src.CLASS, 
                    target.CODE=src.CODE, 
                    target.CONSGRAPHFLAG=src.CONSGRAPHFLAG, 
                    target.CONSUMPTIONPERCENTAGE=src.CONSUMPTIONPERCENTAGE, 
                    target.DELETED=src.DELETED, 
                    target.DONOTPRORATEMOVEINFLAG=src.DONOTPRORATEMOVEINFLAG, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.FIXEDCHGINADVANCEFLAG=src.FIXEDCHGINADVANCEFLAG, 
                    target.FREQUENCY=src.FREQUENCY, 
                    target.IMPERVIOUSAREAONLYFLAG=src.IMPERVIOUSAREAONLYFLAG, 
                    target.LEARNMOREKBKEY=src.LEARNMOREKBKEY, 
                    target.METERTYPEFORMULA=src.METERTYPEFORMULA, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MOVEINACTKEY=src.MOVEINACTKEY, 
                    target.MOVEINCOLOR=src.MOVEINCOLOR, 
                    target.MOVEINNOTES=src.MOVEINNOTES, 
                    target.MOVEINREQSKEY=src.MOVEINREQSKEY, 
                    target.MOVEINSRWOCONDITIONFORMULA=src.MOVEINSRWOCONDITIONFORMULA, 
                    target.MOVEINSWOKEY=src.MOVEINSWOKEY, 
                    target.MOVEOUTACTKEY=src.MOVEOUTACTKEY, 
                    target.MOVEOUTLEARNMOREKBKEY=src.MOVEOUTLEARNMOREKBKEY, 
                    target.MOVEOUTNOTES=src.MOVEOUTNOTES, 
                    target.MOVEOUTREQSKEY=src.MOVEOUTREQSKEY, 
                    target.MOVEOUTSRWOCONDITIONFORMULA=src.MOVEOUTSRWOCONDITIONFORMULA, 
                    target.MOVEOUTSWOKEY=src.MOVEOUTSWOKEY, 
                    target.OUTPUTHISTORICALUSAGEFLAG=src.OUTPUTHISTORICALUSAGEFLAG, 
                    target.OWNERFLAG=src.OWNERFLAG, 
                    target.PRORATESERVICESTARTFLAG=src.PRORATESERVICESTARTFLAG, 
                    target.PRORATESERVICESTOPFLAG=src.PRORATESERVICESTOPFLAG, 
                    target.SERVICECLASS1=src.SERVICECLASS1, 
                    target.SERVICECLASS2=src.SERVICECLASS2, 
                    target.SERVICEDEFINITIONKEY=src.SERVICEDEFINITIONKEY, 
                    target.SERVICEDISTRICT=src.SERVICEDISTRICT, 
                    target.SERVICEFIELD1KEY=src.SERVICEFIELD1KEY, 
                    target.SERVICEFIELD2KEY=src.SERVICEFIELD2KEY, 
                    target.SERVICEFIELD3KEY=src.SERVICEFIELD3KEY, 
                    target.SERVICEOPTIONSKEY=src.SERVICEOPTIONSKEY, 
                    target.SERVICETYPE=src.SERVICETYPE, 
                    target.TENANTFLAG=src.TENANTFLAG, 
                    target.TRADEWASTEFLAG=src.TRADEWASTEFLAG, 
                    target.TRADEWASTEMAX=src.TRADEWASTEMAX, 
                    target.TRADEWASTERATE=src.TRADEWASTERATE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WINTERAVGFLAG=src.WINTERAVGFLAG, 
                    target.WINTERAVGSETUPKEY=src.WINTERAVGSETUPKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, ASSETTYPE, AVAILABILITY, BUILDERFLAG, CLASS, CODE, CONSGRAPHFLAG, CONSUMPTIONPERCENTAGE, DELETED, DONOTPRORATEMOVEINFLAG, EFFDATE, EXPDATE, FIXEDCHGINADVANCEFLAG, FREQUENCY, IMPERVIOUSAREAONLYFLAG, LEARNMOREKBKEY, METERTYPEFORMULA, MODBY, MODDTTM, MOVEINACTKEY, MOVEINCOLOR, MOVEINNOTES, MOVEINREQSKEY, MOVEINSRWOCONDITIONFORMULA, MOVEINSWOKEY, MOVEOUTACTKEY, MOVEOUTLEARNMOREKBKEY, MOVEOUTNOTES, MOVEOUTREQSKEY, MOVEOUTSRWOCONDITIONFORMULA, MOVEOUTSWOKEY, OUTPUTHISTORICALUSAGEFLAG, OWNERFLAG, PRORATESERVICESTARTFLAG, PRORATESERVICESTOPFLAG, SERVICECLASS1, SERVICECLASS2, SERVICEDEFINITIONKEY, SERVICEDISTRICT, SERVICEFIELD1KEY, SERVICEFIELD2KEY, SERVICEFIELD3KEY, SERVICEOPTIONSKEY, SERVICETYPE, TENANTFLAG, TRADEWASTEFLAG, TRADEWASTEMAX, TRADEWASTERATE, VARIATION_ID, WINTERAVGFLAG, WINTERAVGSETUPKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ASSETTYPE, 
                    src.AVAILABILITY, 
                    src.BUILDERFLAG, 
                    src.CLASS, 
                    src.CODE, 
                    src.CONSGRAPHFLAG, 
                    src.CONSUMPTIONPERCENTAGE, 
                    src.DELETED, 
                    src.DONOTPRORATEMOVEINFLAG, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.FIXEDCHGINADVANCEFLAG, 
                    src.FREQUENCY, 
                    src.IMPERVIOUSAREAONLYFLAG, 
                    src.LEARNMOREKBKEY, 
                    src.METERTYPEFORMULA, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MOVEINACTKEY, 
                    src.MOVEINCOLOR, 
                    src.MOVEINNOTES, 
                    src.MOVEINREQSKEY, 
                    src.MOVEINSRWOCONDITIONFORMULA, 
                    src.MOVEINSWOKEY, 
                    src.MOVEOUTACTKEY, 
                    src.MOVEOUTLEARNMOREKBKEY, 
                    src.MOVEOUTNOTES, 
                    src.MOVEOUTREQSKEY, 
                    src.MOVEOUTSRWOCONDITIONFORMULA, 
                    src.MOVEOUTSWOKEY, 
                    src.OUTPUTHISTORICALUSAGEFLAG, 
                    src.OWNERFLAG, 
                    src.PRORATESERVICESTARTFLAG, 
                    src.PRORATESERVICESTOPFLAG, 
                    src.SERVICECLASS1, 
                    src.SERVICECLASS2, 
                    src.SERVICEDEFINITIONKEY, 
                    src.SERVICEDISTRICT, 
                    src.SERVICEFIELD1KEY, 
                    src.SERVICEFIELD2KEY, 
                    src.SERVICEFIELD3KEY, 
                    src.SERVICEOPTIONSKEY, 
                    src.SERVICETYPE, 
                    src.TENANTFLAG, 
                    src.TRADEWASTEFLAG, 
                    src.TRADEWASTEMAX, 
                    src.TRADEWASTERATE, 
                    src.VARIATION_ID, 
                    src.WINTERAVGFLAG, 
                    src.WINTERAVGSETUPKEY,     
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