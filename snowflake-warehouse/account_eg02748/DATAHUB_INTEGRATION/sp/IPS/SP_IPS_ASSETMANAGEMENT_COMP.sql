create or replace procedure DATAHUB_INTEGRATION.SP_IPS_ASSETMANAGEMENT_COMP()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_COMP_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_ASSETMANAGEMENT_COMP as target using (SELECT * FROM (SELECT 
            strm.ACTION, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDRKEY, 
            strm.ADDRQUAL, 
            strm.ALTERNATEID, 
            strm.AREA, 
            strm.BLDGFLOOR, 
            strm.BLDGROOM, 
            strm.COMPKEY, 
            strm.COMPLEXKEY, 
            strm.COMPTYPE, 
            strm.DATALAKE_DELETED, 
            strm.DISTRICT, 
            strm.EXPBY, 
            strm.EXPDATE, 
            strm.INTKEY, 
            strm.LOC, 
            strm.MAINCOMP1, 
            strm.MAINCOMP2, 
            strm.MAINKEY, 
            strm.MAINKEY1, 
            strm.MAINKEY2, 
            strm.MAPNO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PARLINENO, 
            strm.PRCLKEY, 
            strm.RSEGKEY, 
            strm.SEGKEY, 
            strm.SITEKEY, 
            strm.SLKEY, 
            strm.SUBAREA, 
            strm.UNITDESC, 
            strm.UNITID, 
            strm.UNITID2, 
            strm.UNITTYPE, 
            strm.USGAREAKEY, 
            strm.VARIATION_ID, 
            strm.XINGKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.COMPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_COMP as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.COMPKEY=src.COMPKEY) OR (target.COMPKEY IS NULL AND src.COMPKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACTION=src.ACTION, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDRKEY=src.ADDRKEY, 
                    target.ADDRQUAL=src.ADDRQUAL, 
                    target.ALTERNATEID=src.ALTERNATEID, 
                    target.AREA=src.AREA, 
                    target.BLDGFLOOR=src.BLDGFLOOR, 
                    target.BLDGROOM=src.BLDGROOM, 
                    target.COMPKEY=src.COMPKEY, 
                    target.COMPLEXKEY=src.COMPLEXKEY, 
                    target.COMPTYPE=src.COMPTYPE, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DISTRICT=src.DISTRICT, 
                    target.EXPBY=src.EXPBY, 
                    target.EXPDATE=src.EXPDATE, 
                    target.INTKEY=src.INTKEY, 
                    target.LOC=src.LOC, 
                    target.MAINCOMP1=src.MAINCOMP1, 
                    target.MAINCOMP2=src.MAINCOMP2, 
                    target.MAINKEY=src.MAINKEY, 
                    target.MAINKEY1=src.MAINKEY1, 
                    target.MAINKEY2=src.MAINKEY2, 
                    target.MAPNO=src.MAPNO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PARLINENO=src.PARLINENO, 
                    target.PRCLKEY=src.PRCLKEY, 
                    target.RSEGKEY=src.RSEGKEY, 
                    target.SEGKEY=src.SEGKEY, 
                    target.SITEKEY=src.SITEKEY, 
                    target.SLKEY=src.SLKEY, 
                    target.SUBAREA=src.SUBAREA, 
                    target.UNITDESC=src.UNITDESC, 
                    target.UNITID=src.UNITID, 
                    target.UNITID2=src.UNITID2, 
                    target.UNITTYPE=src.UNITTYPE, 
                    target.USGAREAKEY=src.USGAREAKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.XINGKEY=src.XINGKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACTION, 
                    ADDBY, 
                    ADDDTTM, 
                    ADDRKEY, 
                    ADDRQUAL, 
                    ALTERNATEID, 
                    AREA, 
                    BLDGFLOOR, 
                    BLDGROOM, 
                    COMPKEY, 
                    COMPLEXKEY, 
                    COMPTYPE, 
                    DATALAKE_DELETED, 
                    DISTRICT, 
                    EXPBY, 
                    EXPDATE, 
                    INTKEY, 
                    LOC, 
                    MAINCOMP1, 
                    MAINCOMP2, 
                    MAINKEY, 
                    MAINKEY1, 
                    MAINKEY2, 
                    MAPNO, 
                    MODBY, 
                    MODDTTM, 
                    PARLINENO, 
                    PRCLKEY, 
                    RSEGKEY, 
                    SEGKEY, 
                    SITEKEY, 
                    SLKEY, 
                    SUBAREA, 
                    UNITDESC, 
                    UNITID, 
                    UNITID2, 
                    UNITTYPE, 
                    USGAREAKEY, 
                    VARIATION_ID, 
                    XINGKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACTION, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDRKEY, 
                    src.ADDRQUAL, 
                    src.ALTERNATEID, 
                    src.AREA, 
                    src.BLDGFLOOR, 
                    src.BLDGROOM, 
                    src.COMPKEY, 
                    src.COMPLEXKEY, 
                    src.COMPTYPE, 
                    src.DATALAKE_DELETED, 
                    src.DISTRICT, 
                    src.EXPBY, 
                    src.EXPDATE, 
                    src.INTKEY, 
                    src.LOC, 
                    src.MAINCOMP1, 
                    src.MAINCOMP2, 
                    src.MAINKEY, 
                    src.MAINKEY1, 
                    src.MAINKEY2, 
                    src.MAPNO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PARLINENO, 
                    src.PRCLKEY, 
                    src.RSEGKEY, 
                    src.SEGKEY, 
                    src.SITEKEY, 
                    src.SLKEY, 
                    src.SUBAREA, 
                    src.UNITDESC, 
                    src.UNITID, 
                    src.UNITID2, 
                    src.UNITTYPE, 
                    src.USGAREAKEY, 
                    src.VARIATION_ID, 
                    src.XINGKEY,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_ASSETMANAGEMENT_COMP as target using (
                SELECT * FROM (SELECT 
            strm.ACTION, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDRKEY, 
            strm.ADDRQUAL, 
            strm.ALTERNATEID, 
            strm.AREA, 
            strm.BLDGFLOOR, 
            strm.BLDGROOM, 
            strm.COMPKEY, 
            strm.COMPLEXKEY, 
            strm.COMPTYPE, 
            strm.DATALAKE_DELETED, 
            strm.DISTRICT, 
            strm.EXPBY, 
            strm.EXPDATE, 
            strm.INTKEY, 
            strm.LOC, 
            strm.MAINCOMP1, 
            strm.MAINCOMP2, 
            strm.MAINKEY, 
            strm.MAINKEY1, 
            strm.MAINKEY2, 
            strm.MAPNO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PARLINENO, 
            strm.PRCLKEY, 
            strm.RSEGKEY, 
            strm.SEGKEY, 
            strm.SITEKEY, 
            strm.SLKEY, 
            strm.SUBAREA, 
            strm.UNITDESC, 
            strm.UNITID, 
            strm.UNITID2, 
            strm.UNITTYPE, 
            strm.USGAREAKEY, 
            strm.VARIATION_ID, 
            strm.XINGKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.COMPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_COMP as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.COMPKEY=src.COMPKEY) OR (target.COMPKEY IS NULL AND src.COMPKEY IS NULL)) 
                when matched then update set
                    target.ACTION=src.ACTION, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDRKEY=src.ADDRKEY, 
                    target.ADDRQUAL=src.ADDRQUAL, 
                    target.ALTERNATEID=src.ALTERNATEID, 
                    target.AREA=src.AREA, 
                    target.BLDGFLOOR=src.BLDGFLOOR, 
                    target.BLDGROOM=src.BLDGROOM, 
                    target.COMPKEY=src.COMPKEY, 
                    target.COMPLEXKEY=src.COMPLEXKEY, 
                    target.COMPTYPE=src.COMPTYPE, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DISTRICT=src.DISTRICT, 
                    target.EXPBY=src.EXPBY, 
                    target.EXPDATE=src.EXPDATE, 
                    target.INTKEY=src.INTKEY, 
                    target.LOC=src.LOC, 
                    target.MAINCOMP1=src.MAINCOMP1, 
                    target.MAINCOMP2=src.MAINCOMP2, 
                    target.MAINKEY=src.MAINKEY, 
                    target.MAINKEY1=src.MAINKEY1, 
                    target.MAINKEY2=src.MAINKEY2, 
                    target.MAPNO=src.MAPNO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PARLINENO=src.PARLINENO, 
                    target.PRCLKEY=src.PRCLKEY, 
                    target.RSEGKEY=src.RSEGKEY, 
                    target.SEGKEY=src.SEGKEY, 
                    target.SITEKEY=src.SITEKEY, 
                    target.SLKEY=src.SLKEY, 
                    target.SUBAREA=src.SUBAREA, 
                    target.UNITDESC=src.UNITDESC, 
                    target.UNITID=src.UNITID, 
                    target.UNITID2=src.UNITID2, 
                    target.UNITTYPE=src.UNITTYPE, 
                    target.USGAREAKEY=src.USGAREAKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.XINGKEY=src.XINGKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACTION, ADDBY, ADDDTTM, ADDRKEY, ADDRQUAL, ALTERNATEID, AREA, BLDGFLOOR, BLDGROOM, COMPKEY, COMPLEXKEY, COMPTYPE, DATALAKE_DELETED, DISTRICT, EXPBY, EXPDATE, INTKEY, LOC, MAINCOMP1, MAINCOMP2, MAINKEY, MAINKEY1, MAINKEY2, MAPNO, MODBY, MODDTTM, PARLINENO, PRCLKEY, RSEGKEY, SEGKEY, SITEKEY, SLKEY, SUBAREA, UNITDESC, UNITID, UNITID2, UNITTYPE, USGAREAKEY, VARIATION_ID, XINGKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACTION, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDRKEY, 
                    src.ADDRQUAL, 
                    src.ALTERNATEID, 
                    src.AREA, 
                    src.BLDGFLOOR, 
                    src.BLDGROOM, 
                    src.COMPKEY, 
                    src.COMPLEXKEY, 
                    src.COMPTYPE, 
                    src.DATALAKE_DELETED, 
                    src.DISTRICT, 
                    src.EXPBY, 
                    src.EXPDATE, 
                    src.INTKEY, 
                    src.LOC, 
                    src.MAINCOMP1, 
                    src.MAINCOMP2, 
                    src.MAINKEY, 
                    src.MAINKEY1, 
                    src.MAINKEY2, 
                    src.MAPNO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PARLINENO, 
                    src.PRCLKEY, 
                    src.RSEGKEY, 
                    src.SEGKEY, 
                    src.SITEKEY, 
                    src.SLKEY, 
                    src.SUBAREA, 
                    src.UNITDESC, 
                    src.UNITID, 
                    src.UNITID2, 
                    src.UNITTYPE, 
                    src.USGAREAKEY, 
                    src.VARIATION_ID, 
                    src.XINGKEY,     
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