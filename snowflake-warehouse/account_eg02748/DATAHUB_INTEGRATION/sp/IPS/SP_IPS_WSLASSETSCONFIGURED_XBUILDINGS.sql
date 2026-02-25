create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLASSETSCONFIGURED_XBUILDINGS()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLASSETSCONFIGURED_XBUILDINGS_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLASSETSCONFIGURED_XBUILDINGS as target using (SELECT * FROM (SELECT 
            strm.ACCELDEPREC, 
            strm.ACCELDEPRECDESC, 
            strm.ACQTYPE, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AREA, 
            strm.BUSINESSAREA, 
            strm.COMPKEY, 
            strm.CONFINEDSPACE, 
            strm.COSTCENTRE, 
            strm.CRITICALITY, 
            strm.DATALAKE_DELETED, 
            strm.DESIGNRESILIENCE, 
            strm.DISPOSALDESC, 
            strm.DRAWINGNO, 
            strm.DRAWINGNOREF, 
            strm.EXTCOAT, 
            strm.FORDISPOSAL, 
            strm.FUNCAREA, 
            strm.GRDLEVEL, 
            strm.LINKEDASSET, 
            strm.MAKE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MODELNO, 
            strm.PHOTO3D, 
            strm.PROJECTNO, 
            strm.PROJECTWISE, 
            strm.QUAKEDESIGN, 
            strm.RELDATE, 
            strm.RELDEPTH, 
            strm.RELDIA, 
            strm.RELGEOSP, 
            strm.RELMATL, 
            strm.RELOWN, 
            strm.RELSTATUS, 
            strm.SAFETYCRITICAL, 
            strm.VARIATION_ID, 
            strm.WARRANTYEND, 
            strm.WARRANTYSTART, 
            strm.XBUILDINGSKEY, 
            strm.ZONECATCHMENT, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XBUILDINGSKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLASSETSCONFIGURED_XBUILDINGS as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XBUILDINGSKEY=src.XBUILDINGSKEY) OR (target.XBUILDINGSKEY IS NULL AND src.XBUILDINGSKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCELDEPREC=src.ACCELDEPREC, 
                    target.ACCELDEPRECDESC=src.ACCELDEPRECDESC, 
                    target.ACQTYPE=src.ACQTYPE, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AREA=src.AREA, 
                    target.BUSINESSAREA=src.BUSINESSAREA, 
                    target.COMPKEY=src.COMPKEY, 
                    target.CONFINEDSPACE=src.CONFINEDSPACE, 
                    target.COSTCENTRE=src.COSTCENTRE, 
                    target.CRITICALITY=src.CRITICALITY, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DESIGNRESILIENCE=src.DESIGNRESILIENCE, 
                    target.DISPOSALDESC=src.DISPOSALDESC, 
                    target.DRAWINGNO=src.DRAWINGNO, 
                    target.DRAWINGNOREF=src.DRAWINGNOREF, 
                    target.EXTCOAT=src.EXTCOAT, 
                    target.FORDISPOSAL=src.FORDISPOSAL, 
                    target.FUNCAREA=src.FUNCAREA, 
                    target.GRDLEVEL=src.GRDLEVEL, 
                    target.LINKEDASSET=src.LINKEDASSET, 
                    target.MAKE=src.MAKE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MODELNO=src.MODELNO, 
                    target.PHOTO3D=src.PHOTO3D, 
                    target.PROJECTNO=src.PROJECTNO, 
                    target.PROJECTWISE=src.PROJECTWISE, 
                    target.QUAKEDESIGN=src.QUAKEDESIGN, 
                    target.RELDATE=src.RELDATE, 
                    target.RELDEPTH=src.RELDEPTH, 
                    target.RELDIA=src.RELDIA, 
                    target.RELGEOSP=src.RELGEOSP, 
                    target.RELMATL=src.RELMATL, 
                    target.RELOWN=src.RELOWN, 
                    target.RELSTATUS=src.RELSTATUS, 
                    target.SAFETYCRITICAL=src.SAFETYCRITICAL, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WARRANTYEND=src.WARRANTYEND, 
                    target.WARRANTYSTART=src.WARRANTYSTART, 
                    target.XBUILDINGSKEY=src.XBUILDINGSKEY, 
                    target.ZONECATCHMENT=src.ZONECATCHMENT, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ACCELDEPREC, 
                    ACCELDEPRECDESC, 
                    ACQTYPE, 
                    ADDBY, 
                    ADDDTTM, 
                    AREA, 
                    BUSINESSAREA, 
                    COMPKEY, 
                    CONFINEDSPACE, 
                    COSTCENTRE, 
                    CRITICALITY, 
                    DATALAKE_DELETED, 
                    DESIGNRESILIENCE, 
                    DISPOSALDESC, 
                    DRAWINGNO, 
                    DRAWINGNOREF, 
                    EXTCOAT, 
                    FORDISPOSAL, 
                    FUNCAREA, 
                    GRDLEVEL, 
                    LINKEDASSET, 
                    MAKE, 
                    MODBY, 
                    MODDTTM, 
                    MODELNO, 
                    PHOTO3D, 
                    PROJECTNO, 
                    PROJECTWISE, 
                    QUAKEDESIGN, 
                    RELDATE, 
                    RELDEPTH, 
                    RELDIA, 
                    RELGEOSP, 
                    RELMATL, 
                    RELOWN, 
                    RELSTATUS, 
                    SAFETYCRITICAL, 
                    VARIATION_ID, 
                    WARRANTYEND, 
                    WARRANTYSTART, 
                    XBUILDINGSKEY, 
                    ZONECATCHMENT, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ACCELDEPREC, 
                    src.ACCELDEPRECDESC, 
                    src.ACQTYPE, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AREA, 
                    src.BUSINESSAREA, 
                    src.COMPKEY, 
                    src.CONFINEDSPACE, 
                    src.COSTCENTRE, 
                    src.CRITICALITY, 
                    src.DATALAKE_DELETED, 
                    src.DESIGNRESILIENCE, 
                    src.DISPOSALDESC, 
                    src.DRAWINGNO, 
                    src.DRAWINGNOREF, 
                    src.EXTCOAT, 
                    src.FORDISPOSAL, 
                    src.FUNCAREA, 
                    src.GRDLEVEL, 
                    src.LINKEDASSET, 
                    src.MAKE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MODELNO, 
                    src.PHOTO3D, 
                    src.PROJECTNO, 
                    src.PROJECTWISE, 
                    src.QUAKEDESIGN, 
                    src.RELDATE, 
                    src.RELDEPTH, 
                    src.RELDIA, 
                    src.RELGEOSP, 
                    src.RELMATL, 
                    src.RELOWN, 
                    src.RELSTATUS, 
                    src.SAFETYCRITICAL, 
                    src.VARIATION_ID, 
                    src.WARRANTYEND, 
                    src.WARRANTYSTART, 
                    src.XBUILDINGSKEY, 
                    src.ZONECATCHMENT,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLASSETSCONFIGURED_XBUILDINGS as target using (
                SELECT * FROM (SELECT 
            strm.ACCELDEPREC, 
            strm.ACCELDEPRECDESC, 
            strm.ACQTYPE, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AREA, 
            strm.BUSINESSAREA, 
            strm.COMPKEY, 
            strm.CONFINEDSPACE, 
            strm.COSTCENTRE, 
            strm.CRITICALITY, 
            strm.DATALAKE_DELETED, 
            strm.DESIGNRESILIENCE, 
            strm.DISPOSALDESC, 
            strm.DRAWINGNO, 
            strm.DRAWINGNOREF, 
            strm.EXTCOAT, 
            strm.FORDISPOSAL, 
            strm.FUNCAREA, 
            strm.GRDLEVEL, 
            strm.LINKEDASSET, 
            strm.MAKE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MODELNO, 
            strm.PHOTO3D, 
            strm.PROJECTNO, 
            strm.PROJECTWISE, 
            strm.QUAKEDESIGN, 
            strm.RELDATE, 
            strm.RELDEPTH, 
            strm.RELDIA, 
            strm.RELGEOSP, 
            strm.RELMATL, 
            strm.RELOWN, 
            strm.RELSTATUS, 
            strm.SAFETYCRITICAL, 
            strm.VARIATION_ID, 
            strm.WARRANTYEND, 
            strm.WARRANTYSTART, 
            strm.XBUILDINGSKEY, 
            strm.ZONECATCHMENT, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XBUILDINGSKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLASSETSCONFIGURED_XBUILDINGS as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XBUILDINGSKEY=src.XBUILDINGSKEY) OR (target.XBUILDINGSKEY IS NULL AND src.XBUILDINGSKEY IS NULL)) 
                when matched then update set
                    target.ACCELDEPREC=src.ACCELDEPREC, 
                    target.ACCELDEPRECDESC=src.ACCELDEPRECDESC, 
                    target.ACQTYPE=src.ACQTYPE, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AREA=src.AREA, 
                    target.BUSINESSAREA=src.BUSINESSAREA, 
                    target.COMPKEY=src.COMPKEY, 
                    target.CONFINEDSPACE=src.CONFINEDSPACE, 
                    target.COSTCENTRE=src.COSTCENTRE, 
                    target.CRITICALITY=src.CRITICALITY, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DESIGNRESILIENCE=src.DESIGNRESILIENCE, 
                    target.DISPOSALDESC=src.DISPOSALDESC, 
                    target.DRAWINGNO=src.DRAWINGNO, 
                    target.DRAWINGNOREF=src.DRAWINGNOREF, 
                    target.EXTCOAT=src.EXTCOAT, 
                    target.FORDISPOSAL=src.FORDISPOSAL, 
                    target.FUNCAREA=src.FUNCAREA, 
                    target.GRDLEVEL=src.GRDLEVEL, 
                    target.LINKEDASSET=src.LINKEDASSET, 
                    target.MAKE=src.MAKE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MODELNO=src.MODELNO, 
                    target.PHOTO3D=src.PHOTO3D, 
                    target.PROJECTNO=src.PROJECTNO, 
                    target.PROJECTWISE=src.PROJECTWISE, 
                    target.QUAKEDESIGN=src.QUAKEDESIGN, 
                    target.RELDATE=src.RELDATE, 
                    target.RELDEPTH=src.RELDEPTH, 
                    target.RELDIA=src.RELDIA, 
                    target.RELGEOSP=src.RELGEOSP, 
                    target.RELMATL=src.RELMATL, 
                    target.RELOWN=src.RELOWN, 
                    target.RELSTATUS=src.RELSTATUS, 
                    target.SAFETYCRITICAL=src.SAFETYCRITICAL, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WARRANTYEND=src.WARRANTYEND, 
                    target.WARRANTYSTART=src.WARRANTYSTART, 
                    target.XBUILDINGSKEY=src.XBUILDINGSKEY, 
                    target.ZONECATCHMENT=src.ZONECATCHMENT, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCELDEPREC, ACCELDEPRECDESC, ACQTYPE, ADDBY, ADDDTTM, AREA, BUSINESSAREA, COMPKEY, CONFINEDSPACE, COSTCENTRE, CRITICALITY, DATALAKE_DELETED, DESIGNRESILIENCE, DISPOSALDESC, DRAWINGNO, DRAWINGNOREF, EXTCOAT, FORDISPOSAL, FUNCAREA, GRDLEVEL, LINKEDASSET, MAKE, MODBY, MODDTTM, MODELNO, PHOTO3D, PROJECTNO, PROJECTWISE, QUAKEDESIGN, RELDATE, RELDEPTH, RELDIA, RELGEOSP, RELMATL, RELOWN, RELSTATUS, SAFETYCRITICAL, VARIATION_ID, WARRANTYEND, WARRANTYSTART, XBUILDINGSKEY, ZONECATCHMENT, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ACCELDEPREC, 
                    src.ACCELDEPRECDESC, 
                    src.ACQTYPE, 
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AREA, 
                    src.BUSINESSAREA, 
                    src.COMPKEY, 
                    src.CONFINEDSPACE, 
                    src.COSTCENTRE, 
                    src.CRITICALITY, 
                    src.DATALAKE_DELETED, 
                    src.DESIGNRESILIENCE, 
                    src.DISPOSALDESC, 
                    src.DRAWINGNO, 
                    src.DRAWINGNOREF, 
                    src.EXTCOAT, 
                    src.FORDISPOSAL, 
                    src.FUNCAREA, 
                    src.GRDLEVEL, 
                    src.LINKEDASSET, 
                    src.MAKE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MODELNO, 
                    src.PHOTO3D, 
                    src.PROJECTNO, 
                    src.PROJECTWISE, 
                    src.QUAKEDESIGN, 
                    src.RELDATE, 
                    src.RELDEPTH, 
                    src.RELDIA, 
                    src.RELGEOSP, 
                    src.RELMATL, 
                    src.RELOWN, 
                    src.RELSTATUS, 
                    src.SAFETYCRITICAL, 
                    src.VARIATION_ID, 
                    src.WARRANTYEND, 
                    src.WARRANTYSTART, 
                    src.XBUILDINGSKEY, 
                    src.ZONECATCHMENT,     
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