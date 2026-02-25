create or replace procedure DATAHUB_INTEGRATION.SP_IPS_ASSETMANAGEMENT_WATER_METERREADING()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_WATER_METERREADING_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_ASSETMANAGEMENT_WATER_METERREADING as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AUDITFLAG, 
            strm.BLUSAGE, 
            strm.BLUSAGECUBICFT, 
            strm.COMPKEY, 
            strm.CORRFLAG, 
            strm.CUSTPROB, 
            strm.CYCLE, 
            strm.DELETED, 
            strm.ESTMFLAG, 
            strm.FIELDNOTES, 
            strm.FINALFLAG, 
            strm.GRPPROJ, 
            strm.HIGHBLUSAGE, 
            strm.HIGHBLUSAGEINCUBICFEET, 
            strm.HIGHREADING, 
            strm.HIGHUSAGE, 
            strm.HISTKEY, 
            strm.INITFLAG, 
            strm.INSPKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NOREADFLAG, 
            strm.PROBBCODEWO, 
            strm.PROBCODESRVREQNO, 
            strm.PROBLEMCODE, 
            strm.RDRCODESRVREQNO, 
            strm.READBY, 
            strm.READDTTM, 
            strm.READERCODE, 
            strm.READERCODEWO, 
            strm.READING, 
            strm.READINGKEY, 
            strm.READREAS, 
            strm.READSRC, 
            strm.READVAL1, 
            strm.READVAL2, 
            strm.READVAL3, 
            strm.READVAL4, 
            strm.READYFLAG, 
            strm.USAGE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.READINGKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_WATER_METERREADING as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.READINGKEY=src.READINGKEY) OR (target.READINGKEY IS NULL AND src.READINGKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AUDITFLAG=src.AUDITFLAG, 
                    target.BLUSAGE=src.BLUSAGE, 
                    target.BLUSAGECUBICFT=src.BLUSAGECUBICFT, 
                    target.COMPKEY=src.COMPKEY, 
                    target.CORRFLAG=src.CORRFLAG, 
                    target.CUSTPROB=src.CUSTPROB, 
                    target.CYCLE=src.CYCLE, 
                    target.DELETED=src.DELETED, 
                    target.ESTMFLAG=src.ESTMFLAG, 
                    target.FIELDNOTES=src.FIELDNOTES, 
                    target.FINALFLAG=src.FINALFLAG, 
                    target.GRPPROJ=src.GRPPROJ, 
                    target.HIGHBLUSAGE=src.HIGHBLUSAGE, 
                    target.HIGHBLUSAGEINCUBICFEET=src.HIGHBLUSAGEINCUBICFEET, 
                    target.HIGHREADING=src.HIGHREADING, 
                    target.HIGHUSAGE=src.HIGHUSAGE, 
                    target.HISTKEY=src.HISTKEY, 
                    target.INITFLAG=src.INITFLAG, 
                    target.INSPKEY=src.INSPKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NOREADFLAG=src.NOREADFLAG, 
                    target.PROBBCODEWO=src.PROBBCODEWO, 
                    target.PROBCODESRVREQNO=src.PROBCODESRVREQNO, 
                    target.PROBLEMCODE=src.PROBLEMCODE, 
                    target.RDRCODESRVREQNO=src.RDRCODESRVREQNO, 
                    target.READBY=src.READBY, 
                    target.READDTTM=src.READDTTM, 
                    target.READERCODE=src.READERCODE, 
                    target.READERCODEWO=src.READERCODEWO, 
                    target.READING=src.READING, 
                    target.READINGKEY=src.READINGKEY, 
                    target.READREAS=src.READREAS, 
                    target.READSRC=src.READSRC, 
                    target.READVAL1=src.READVAL1, 
                    target.READVAL2=src.READVAL2, 
                    target.READVAL3=src.READVAL3, 
                    target.READVAL4=src.READVAL4, 
                    target.READYFLAG=src.READYFLAG, 
                    target.USAGE=src.USAGE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    AUDITFLAG, 
                    BLUSAGE, 
                    BLUSAGECUBICFT, 
                    COMPKEY, 
                    CORRFLAG, 
                    CUSTPROB, 
                    CYCLE, 
                    DELETED, 
                    ESTMFLAG, 
                    FIELDNOTES, 
                    FINALFLAG, 
                    GRPPROJ, 
                    HIGHBLUSAGE, 
                    HIGHBLUSAGEINCUBICFEET, 
                    HIGHREADING, 
                    HIGHUSAGE, 
                    HISTKEY, 
                    INITFLAG, 
                    INSPKEY, 
                    MODBY, 
                    MODDTTM, 
                    NOREADFLAG, 
                    PROBBCODEWO, 
                    PROBCODESRVREQNO, 
                    PROBLEMCODE, 
                    RDRCODESRVREQNO, 
                    READBY, 
                    READDTTM, 
                    READERCODE, 
                    READERCODEWO, 
                    READING, 
                    READINGKEY, 
                    READREAS, 
                    READSRC, 
                    READVAL1, 
                    READVAL2, 
                    READVAL3, 
                    READVAL4, 
                    READYFLAG, 
                    USAGE, 
                    VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AUDITFLAG, 
                    src.BLUSAGE, 
                    src.BLUSAGECUBICFT, 
                    src.COMPKEY, 
                    src.CORRFLAG, 
                    src.CUSTPROB, 
                    src.CYCLE, 
                    src.DELETED, 
                    src.ESTMFLAG, 
                    src.FIELDNOTES, 
                    src.FINALFLAG, 
                    src.GRPPROJ, 
                    src.HIGHBLUSAGE, 
                    src.HIGHBLUSAGEINCUBICFEET, 
                    src.HIGHREADING, 
                    src.HIGHUSAGE, 
                    src.HISTKEY, 
                    src.INITFLAG, 
                    src.INSPKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NOREADFLAG, 
                    src.PROBBCODEWO, 
                    src.PROBCODESRVREQNO, 
                    src.PROBLEMCODE, 
                    src.RDRCODESRVREQNO, 
                    src.READBY, 
                    src.READDTTM, 
                    src.READERCODE, 
                    src.READERCODEWO, 
                    src.READING, 
                    src.READINGKEY, 
                    src.READREAS, 
                    src.READSRC, 
                    src.READVAL1, 
                    src.READVAL2, 
                    src.READVAL3, 
                    src.READVAL4, 
                    src.READYFLAG, 
                    src.USAGE, 
                    src.VARIATION_ID,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_ASSETMANAGEMENT_WATER_METERREADING as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AUDITFLAG, 
            strm.BLUSAGE, 
            strm.BLUSAGECUBICFT, 
            strm.COMPKEY, 
            strm.CORRFLAG, 
            strm.CUSTPROB, 
            strm.CYCLE, 
            strm.DELETED, 
            strm.ESTMFLAG, 
            strm.FIELDNOTES, 
            strm.FINALFLAG, 
            strm.GRPPROJ, 
            strm.HIGHBLUSAGE, 
            strm.HIGHBLUSAGEINCUBICFEET, 
            strm.HIGHREADING, 
            strm.HIGHUSAGE, 
            strm.HISTKEY, 
            strm.INITFLAG, 
            strm.INSPKEY, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NOREADFLAG, 
            strm.PROBBCODEWO, 
            strm.PROBCODESRVREQNO, 
            strm.PROBLEMCODE, 
            strm.RDRCODESRVREQNO, 
            strm.READBY, 
            strm.READDTTM, 
            strm.READERCODE, 
            strm.READERCODEWO, 
            strm.READING, 
            strm.READINGKEY, 
            strm.READREAS, 
            strm.READSRC, 
            strm.READVAL1, 
            strm.READVAL2, 
            strm.READVAL3, 
            strm.READVAL4, 
            strm.READYFLAG, 
            strm.USAGE, 
            strm.VARIATION_ID, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.READINGKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_WATER_METERREADING as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.READINGKEY=src.READINGKEY) OR (target.READINGKEY IS NULL AND src.READINGKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AUDITFLAG=src.AUDITFLAG, 
                    target.BLUSAGE=src.BLUSAGE, 
                    target.BLUSAGECUBICFT=src.BLUSAGECUBICFT, 
                    target.COMPKEY=src.COMPKEY, 
                    target.CORRFLAG=src.CORRFLAG, 
                    target.CUSTPROB=src.CUSTPROB, 
                    target.CYCLE=src.CYCLE, 
                    target.DELETED=src.DELETED, 
                    target.ESTMFLAG=src.ESTMFLAG, 
                    target.FIELDNOTES=src.FIELDNOTES, 
                    target.FINALFLAG=src.FINALFLAG, 
                    target.GRPPROJ=src.GRPPROJ, 
                    target.HIGHBLUSAGE=src.HIGHBLUSAGE, 
                    target.HIGHBLUSAGEINCUBICFEET=src.HIGHBLUSAGEINCUBICFEET, 
                    target.HIGHREADING=src.HIGHREADING, 
                    target.HIGHUSAGE=src.HIGHUSAGE, 
                    target.HISTKEY=src.HISTKEY, 
                    target.INITFLAG=src.INITFLAG, 
                    target.INSPKEY=src.INSPKEY, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NOREADFLAG=src.NOREADFLAG, 
                    target.PROBBCODEWO=src.PROBBCODEWO, 
                    target.PROBCODESRVREQNO=src.PROBCODESRVREQNO, 
                    target.PROBLEMCODE=src.PROBLEMCODE, 
                    target.RDRCODESRVREQNO=src.RDRCODESRVREQNO, 
                    target.READBY=src.READBY, 
                    target.READDTTM=src.READDTTM, 
                    target.READERCODE=src.READERCODE, 
                    target.READERCODEWO=src.READERCODEWO, 
                    target.READING=src.READING, 
                    target.READINGKEY=src.READINGKEY, 
                    target.READREAS=src.READREAS, 
                    target.READSRC=src.READSRC, 
                    target.READVAL1=src.READVAL1, 
                    target.READVAL2=src.READVAL2, 
                    target.READVAL3=src.READVAL3, 
                    target.READVAL4=src.READVAL4, 
                    target.READYFLAG=src.READYFLAG, 
                    target.USAGE=src.USAGE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, AUDITFLAG, BLUSAGE, BLUSAGECUBICFT, COMPKEY, CORRFLAG, CUSTPROB, CYCLE, DELETED, ESTMFLAG, FIELDNOTES, FINALFLAG, GRPPROJ, HIGHBLUSAGE, HIGHBLUSAGEINCUBICFEET, HIGHREADING, HIGHUSAGE, HISTKEY, INITFLAG, INSPKEY, MODBY, MODDTTM, NOREADFLAG, PROBBCODEWO, PROBCODESRVREQNO, PROBLEMCODE, RDRCODESRVREQNO, READBY, READDTTM, READERCODE, READERCODEWO, READING, READINGKEY, READREAS, READSRC, READVAL1, READVAL2, READVAL3, READVAL4, READYFLAG, USAGE, VARIATION_ID, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AUDITFLAG, 
                    src.BLUSAGE, 
                    src.BLUSAGECUBICFT, 
                    src.COMPKEY, 
                    src.CORRFLAG, 
                    src.CUSTPROB, 
                    src.CYCLE, 
                    src.DELETED, 
                    src.ESTMFLAG, 
                    src.FIELDNOTES, 
                    src.FINALFLAG, 
                    src.GRPPROJ, 
                    src.HIGHBLUSAGE, 
                    src.HIGHBLUSAGEINCUBICFEET, 
                    src.HIGHREADING, 
                    src.HIGHUSAGE, 
                    src.HISTKEY, 
                    src.INITFLAG, 
                    src.INSPKEY, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NOREADFLAG, 
                    src.PROBBCODEWO, 
                    src.PROBCODESRVREQNO, 
                    src.PROBLEMCODE, 
                    src.RDRCODESRVREQNO, 
                    src.READBY, 
                    src.READDTTM, 
                    src.READERCODE, 
                    src.READERCODEWO, 
                    src.READING, 
                    src.READINGKEY, 
                    src.READREAS, 
                    src.READSRC, 
                    src.READVAL1, 
                    src.READVAL2, 
                    src.READVAL3, 
                    src.READVAL4, 
                    src.READYFLAG, 
                    src.USAGE, 
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