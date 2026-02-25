create or replace procedure DATAHUB_INTEGRATION.SP_IPS_ASSETMANAGEMENT_SEWER_COMPLS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_ASSETMANAGEMENT_SEWER_COMPLS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_ASSETMANAGEMENT_SEWER_COMPLS as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDRKEY, 
            strm.ADDRQUAL, 
            strm.AREA, 
            strm.ASBLT, 
            strm.BGTNO, 
            strm.BLDGFLOOR, 
            strm.BLDGROOM, 
            strm.BUDGETKEY, 
            strm.COMPKEY, 
            strm.COMPLEXKEY, 
            strm.DATALAKE_DELETED, 
            strm.DISTRICT, 
            strm.EXPBY, 
            strm.EXPDATE, 
            strm.GISSTATIC, 
            strm.INSTDATE, 
            strm.LOC, 
            strm.LSDESC, 
            strm.MAINKEY, 
            strm.MAPNO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MODELNO, 
            strm.NOPUMPS, 
            strm.OVFLELEV, 
            strm.OVFLELEVUOM, 
            strm.OWN, 
            strm.POSITION, 
            strm.PRCLKEY, 
            strm.PUDISSIZE, 
            strm.PUDISSIZEUOM, 
            strm.PUMPCAP, 
            strm.PUMPCAPUOM, 
            strm.SEGKEY, 
            strm.SERNO, 
            strm.SERVSTAT, 
            strm.SITEKEY, 
            strm.SUBAREA, 
            strm.UNITDESC, 
            strm.UNITID, 
            strm.UNITTYPE, 
            strm.USGAREAKEY, 
            strm.VARIATION_ID, 
            strm.WETWLELEV, 
            strm.WETWLELEVUOM, 
            strm.WETWLVOL, 
            strm.WETWLVOLUOM, 
            strm.XCOORD, 
            strm.YCOORD, 
            strm.ZCOORD, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.COMPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_SEWER_COMPLS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.COMPKEY=src.COMPKEY) OR (target.COMPKEY IS NULL AND src.COMPKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDRKEY=src.ADDRKEY, 
                    target.ADDRQUAL=src.ADDRQUAL, 
                    target.AREA=src.AREA, 
                    target.ASBLT=src.ASBLT, 
                    target.BGTNO=src.BGTNO, 
                    target.BLDGFLOOR=src.BLDGFLOOR, 
                    target.BLDGROOM=src.BLDGROOM, 
                    target.BUDGETKEY=src.BUDGETKEY, 
                    target.COMPKEY=src.COMPKEY, 
                    target.COMPLEXKEY=src.COMPLEXKEY, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DISTRICT=src.DISTRICT, 
                    target.EXPBY=src.EXPBY, 
                    target.EXPDATE=src.EXPDATE, 
                    target.GISSTATIC=src.GISSTATIC, 
                    target.INSTDATE=src.INSTDATE, 
                    target.LOC=src.LOC, 
                    target.LSDESC=src.LSDESC, 
                    target.MAINKEY=src.MAINKEY, 
                    target.MAPNO=src.MAPNO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MODELNO=src.MODELNO, 
                    target.NOPUMPS=src.NOPUMPS, 
                    target.OVFLELEV=src.OVFLELEV, 
                    target.OVFLELEVUOM=src.OVFLELEVUOM, 
                    target.OWN=src.OWN, 
                    target.POSITION=src.POSITION, 
                    target.PRCLKEY=src.PRCLKEY, 
                    target.PUDISSIZE=src.PUDISSIZE, 
                    target.PUDISSIZEUOM=src.PUDISSIZEUOM, 
                    target.PUMPCAP=src.PUMPCAP, 
                    target.PUMPCAPUOM=src.PUMPCAPUOM, 
                    target.SEGKEY=src.SEGKEY, 
                    target.SERNO=src.SERNO, 
                    target.SERVSTAT=src.SERVSTAT, 
                    target.SITEKEY=src.SITEKEY, 
                    target.SUBAREA=src.SUBAREA, 
                    target.UNITDESC=src.UNITDESC, 
                    target.UNITID=src.UNITID, 
                    target.UNITTYPE=src.UNITTYPE, 
                    target.USGAREAKEY=src.USGAREAKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WETWLELEV=src.WETWLELEV, 
                    target.WETWLELEVUOM=src.WETWLELEVUOM, 
                    target.WETWLVOL=src.WETWLVOL, 
                    target.WETWLVOLUOM=src.WETWLVOLUOM, 
                    target.XCOORD=src.XCOORD, 
                    target.YCOORD=src.YCOORD, 
                    target.ZCOORD=src.ZCOORD, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    ADDRKEY, 
                    ADDRQUAL, 
                    AREA, 
                    ASBLT, 
                    BGTNO, 
                    BLDGFLOOR, 
                    BLDGROOM, 
                    BUDGETKEY, 
                    COMPKEY, 
                    COMPLEXKEY, 
                    DATALAKE_DELETED, 
                    DISTRICT, 
                    EXPBY, 
                    EXPDATE, 
                    GISSTATIC, 
                    INSTDATE, 
                    LOC, 
                    LSDESC, 
                    MAINKEY, 
                    MAPNO, 
                    MODBY, 
                    MODDTTM, 
                    MODELNO, 
                    NOPUMPS, 
                    OVFLELEV, 
                    OVFLELEVUOM, 
                    OWN, 
                    POSITION, 
                    PRCLKEY, 
                    PUDISSIZE, 
                    PUDISSIZEUOM, 
                    PUMPCAP, 
                    PUMPCAPUOM, 
                    SEGKEY, 
                    SERNO, 
                    SERVSTAT, 
                    SITEKEY, 
                    SUBAREA, 
                    UNITDESC, 
                    UNITID, 
                    UNITTYPE, 
                    USGAREAKEY, 
                    VARIATION_ID, 
                    WETWLELEV, 
                    WETWLELEVUOM, 
                    WETWLVOL, 
                    WETWLVOLUOM, 
                    XCOORD, 
                    YCOORD, 
                    ZCOORD, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDRKEY, 
                    src.ADDRQUAL, 
                    src.AREA, 
                    src.ASBLT, 
                    src.BGTNO, 
                    src.BLDGFLOOR, 
                    src.BLDGROOM, 
                    src.BUDGETKEY, 
                    src.COMPKEY, 
                    src.COMPLEXKEY, 
                    src.DATALAKE_DELETED, 
                    src.DISTRICT, 
                    src.EXPBY, 
                    src.EXPDATE, 
                    src.GISSTATIC, 
                    src.INSTDATE, 
                    src.LOC, 
                    src.LSDESC, 
                    src.MAINKEY, 
                    src.MAPNO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MODELNO, 
                    src.NOPUMPS, 
                    src.OVFLELEV, 
                    src.OVFLELEVUOM, 
                    src.OWN, 
                    src.POSITION, 
                    src.PRCLKEY, 
                    src.PUDISSIZE, 
                    src.PUDISSIZEUOM, 
                    src.PUMPCAP, 
                    src.PUMPCAPUOM, 
                    src.SEGKEY, 
                    src.SERNO, 
                    src.SERVSTAT, 
                    src.SITEKEY, 
                    src.SUBAREA, 
                    src.UNITDESC, 
                    src.UNITID, 
                    src.UNITTYPE, 
                    src.USGAREAKEY, 
                    src.VARIATION_ID, 
                    src.WETWLELEV, 
                    src.WETWLELEVUOM, 
                    src.WETWLVOL, 
                    src.WETWLVOLUOM, 
                    src.XCOORD, 
                    src.YCOORD, 
                    src.ZCOORD,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_ASSETMANAGEMENT_SEWER_COMPLS as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ADDRKEY, 
            strm.ADDRQUAL, 
            strm.AREA, 
            strm.ASBLT, 
            strm.BGTNO, 
            strm.BLDGFLOOR, 
            strm.BLDGROOM, 
            strm.BUDGETKEY, 
            strm.COMPKEY, 
            strm.COMPLEXKEY, 
            strm.DATALAKE_DELETED, 
            strm.DISTRICT, 
            strm.EXPBY, 
            strm.EXPDATE, 
            strm.GISSTATIC, 
            strm.INSTDATE, 
            strm.LOC, 
            strm.LSDESC, 
            strm.MAINKEY, 
            strm.MAPNO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MODELNO, 
            strm.NOPUMPS, 
            strm.OVFLELEV, 
            strm.OVFLELEVUOM, 
            strm.OWN, 
            strm.POSITION, 
            strm.PRCLKEY, 
            strm.PUDISSIZE, 
            strm.PUDISSIZEUOM, 
            strm.PUMPCAP, 
            strm.PUMPCAPUOM, 
            strm.SEGKEY, 
            strm.SERNO, 
            strm.SERVSTAT, 
            strm.SITEKEY, 
            strm.SUBAREA, 
            strm.UNITDESC, 
            strm.UNITID, 
            strm.UNITTYPE, 
            strm.USGAREAKEY, 
            strm.VARIATION_ID, 
            strm.WETWLELEV, 
            strm.WETWLELEVUOM, 
            strm.WETWLVOL, 
            strm.WETWLVOLUOM, 
            strm.XCOORD, 
            strm.YCOORD, 
            strm.ZCOORD, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.COMPKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_ASSETMANAGEMENT_SEWER_COMPLS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.COMPKEY=src.COMPKEY) OR (target.COMPKEY IS NULL AND src.COMPKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ADDRKEY=src.ADDRKEY, 
                    target.ADDRQUAL=src.ADDRQUAL, 
                    target.AREA=src.AREA, 
                    target.ASBLT=src.ASBLT, 
                    target.BGTNO=src.BGTNO, 
                    target.BLDGFLOOR=src.BLDGFLOOR, 
                    target.BLDGROOM=src.BLDGROOM, 
                    target.BUDGETKEY=src.BUDGETKEY, 
                    target.COMPKEY=src.COMPKEY, 
                    target.COMPLEXKEY=src.COMPLEXKEY, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DISTRICT=src.DISTRICT, 
                    target.EXPBY=src.EXPBY, 
                    target.EXPDATE=src.EXPDATE, 
                    target.GISSTATIC=src.GISSTATIC, 
                    target.INSTDATE=src.INSTDATE, 
                    target.LOC=src.LOC, 
                    target.LSDESC=src.LSDESC, 
                    target.MAINKEY=src.MAINKEY, 
                    target.MAPNO=src.MAPNO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MODELNO=src.MODELNO, 
                    target.NOPUMPS=src.NOPUMPS, 
                    target.OVFLELEV=src.OVFLELEV, 
                    target.OVFLELEVUOM=src.OVFLELEVUOM, 
                    target.OWN=src.OWN, 
                    target.POSITION=src.POSITION, 
                    target.PRCLKEY=src.PRCLKEY, 
                    target.PUDISSIZE=src.PUDISSIZE, 
                    target.PUDISSIZEUOM=src.PUDISSIZEUOM, 
                    target.PUMPCAP=src.PUMPCAP, 
                    target.PUMPCAPUOM=src.PUMPCAPUOM, 
                    target.SEGKEY=src.SEGKEY, 
                    target.SERNO=src.SERNO, 
                    target.SERVSTAT=src.SERVSTAT, 
                    target.SITEKEY=src.SITEKEY, 
                    target.SUBAREA=src.SUBAREA, 
                    target.UNITDESC=src.UNITDESC, 
                    target.UNITID=src.UNITID, 
                    target.UNITTYPE=src.UNITTYPE, 
                    target.USGAREAKEY=src.USGAREAKEY, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WETWLELEV=src.WETWLELEV, 
                    target.WETWLELEVUOM=src.WETWLELEVUOM, 
                    target.WETWLVOL=src.WETWLVOL, 
                    target.WETWLVOLUOM=src.WETWLVOLUOM, 
                    target.XCOORD=src.XCOORD, 
                    target.YCOORD=src.YCOORD, 
                    target.ZCOORD=src.ZCOORD, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, ADDRKEY, ADDRQUAL, AREA, ASBLT, BGTNO, BLDGFLOOR, BLDGROOM, BUDGETKEY, COMPKEY, COMPLEXKEY, DATALAKE_DELETED, DISTRICT, EXPBY, EXPDATE, GISSTATIC, INSTDATE, LOC, LSDESC, MAINKEY, MAPNO, MODBY, MODDTTM, MODELNO, NOPUMPS, OVFLELEV, OVFLELEVUOM, OWN, POSITION, PRCLKEY, PUDISSIZE, PUDISSIZEUOM, PUMPCAP, PUMPCAPUOM, SEGKEY, SERNO, SERVSTAT, SITEKEY, SUBAREA, UNITDESC, UNITID, UNITTYPE, USGAREAKEY, VARIATION_ID, WETWLELEV, WETWLELEVUOM, WETWLVOL, WETWLVOLUOM, XCOORD, YCOORD, ZCOORD, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ADDRKEY, 
                    src.ADDRQUAL, 
                    src.AREA, 
                    src.ASBLT, 
                    src.BGTNO, 
                    src.BLDGFLOOR, 
                    src.BLDGROOM, 
                    src.BUDGETKEY, 
                    src.COMPKEY, 
                    src.COMPLEXKEY, 
                    src.DATALAKE_DELETED, 
                    src.DISTRICT, 
                    src.EXPBY, 
                    src.EXPDATE, 
                    src.GISSTATIC, 
                    src.INSTDATE, 
                    src.LOC, 
                    src.LSDESC, 
                    src.MAINKEY, 
                    src.MAPNO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MODELNO, 
                    src.NOPUMPS, 
                    src.OVFLELEV, 
                    src.OVFLELEVUOM, 
                    src.OWN, 
                    src.POSITION, 
                    src.PRCLKEY, 
                    src.PUDISSIZE, 
                    src.PUDISSIZEUOM, 
                    src.PUMPCAP, 
                    src.PUMPCAPUOM, 
                    src.SEGKEY, 
                    src.SERNO, 
                    src.SERVSTAT, 
                    src.SITEKEY, 
                    src.SUBAREA, 
                    src.UNITDESC, 
                    src.UNITID, 
                    src.UNITTYPE, 
                    src.USGAREAKEY, 
                    src.VARIATION_ID, 
                    src.WETWLELEV, 
                    src.WETWLELEVUOM, 
                    src.WETWLVOL, 
                    src.WETWLVOLUOM, 
                    src.XCOORD, 
                    src.YCOORD, 
                    src.ZCOORD,     
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