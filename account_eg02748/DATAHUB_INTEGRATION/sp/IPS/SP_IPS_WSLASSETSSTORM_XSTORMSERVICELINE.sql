create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLASSETSSTORM_XSTORMSERVICELINE()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLASSETSSTORM_XSTORMSERVICELINE_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLASSETSSTORM_XSTORMSERVICELINE as target using (SELECT * FROM (SELECT 
            strm.ACCELDEPREC, 
            strm.ACCELDEPRECDESC, 
            strm.ACQTYPE, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BUSINESSAREA, 
            strm.COMPKEY, 
            strm.CONFINEDSPACE, 
            strm.CONSTMETHOD, 
            strm.COSTCENTRE, 
            strm.CRITICALITY, 
            strm.DATALAKE_DELETED, 
            strm.DESIGNRESILIENCE, 
            strm.DIAMEXT, 
            strm.DIAMINT, 
            strm.DISPOSALDESC, 
            strm.DRAWINGNO, 
            strm.DRAWINGNOREF, 
            strm.EXTCOAT, 
            strm.FORDISPOSAL, 
            strm.FUNCAREA, 
            strm.GRDLEVEL, 
            strm.INTLINING, 
            strm.INVLEVEL, 
            strm.JTTYPE, 
            strm.LINKEDASSET, 
            strm.LOCALITY, 
            strm.MAKE, 
            strm.MEDIATYPE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MODELNO, 
            strm.PHOTO3D, 
            strm.PRESSURERATING, 
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
            strm.STIFFRATING, 
            strm.VARIATION_ID, 
            strm.WARRANTYEND, 
            strm.WARRANTYSTART, 
            strm.XSTORMSERVICELINEKEY, 
            strm.ZONECATCHMENT, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XSTORMSERVICELINEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLASSETSSTORM_XSTORMSERVICELINE as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XSTORMSERVICELINEKEY=src.XSTORMSERVICELINEKEY) OR (target.XSTORMSERVICELINEKEY IS NULL AND src.XSTORMSERVICELINEKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ACCELDEPREC=src.ACCELDEPREC, 
                    target.ACCELDEPRECDESC=src.ACCELDEPRECDESC, 
                    target.ACQTYPE=src.ACQTYPE, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BUSINESSAREA=src.BUSINESSAREA, 
                    target.COMPKEY=src.COMPKEY, 
                    target.CONFINEDSPACE=src.CONFINEDSPACE, 
                    target.CONSTMETHOD=src.CONSTMETHOD, 
                    target.COSTCENTRE=src.COSTCENTRE, 
                    target.CRITICALITY=src.CRITICALITY, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DESIGNRESILIENCE=src.DESIGNRESILIENCE, 
                    target.DIAMEXT=src.DIAMEXT, 
                    target.DIAMINT=src.DIAMINT, 
                    target.DISPOSALDESC=src.DISPOSALDESC, 
                    target.DRAWINGNO=src.DRAWINGNO, 
                    target.DRAWINGNOREF=src.DRAWINGNOREF, 
                    target.EXTCOAT=src.EXTCOAT, 
                    target.FORDISPOSAL=src.FORDISPOSAL, 
                    target.FUNCAREA=src.FUNCAREA, 
                    target.GRDLEVEL=src.GRDLEVEL, 
                    target.INTLINING=src.INTLINING, 
                    target.INVLEVEL=src.INVLEVEL, 
                    target.JTTYPE=src.JTTYPE, 
                    target.LINKEDASSET=src.LINKEDASSET, 
                    target.LOCALITY=src.LOCALITY, 
                    target.MAKE=src.MAKE, 
                    target.MEDIATYPE=src.MEDIATYPE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MODELNO=src.MODELNO, 
                    target.PHOTO3D=src.PHOTO3D, 
                    target.PRESSURERATING=src.PRESSURERATING, 
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
                    target.STIFFRATING=src.STIFFRATING, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WARRANTYEND=src.WARRANTYEND, 
                    target.WARRANTYSTART=src.WARRANTYSTART, 
                    target.XSTORMSERVICELINEKEY=src.XSTORMSERVICELINEKEY, 
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
                    BUSINESSAREA, 
                    COMPKEY, 
                    CONFINEDSPACE, 
                    CONSTMETHOD, 
                    COSTCENTRE, 
                    CRITICALITY, 
                    DATALAKE_DELETED, 
                    DESIGNRESILIENCE, 
                    DIAMEXT, 
                    DIAMINT, 
                    DISPOSALDESC, 
                    DRAWINGNO, 
                    DRAWINGNOREF, 
                    EXTCOAT, 
                    FORDISPOSAL, 
                    FUNCAREA, 
                    GRDLEVEL, 
                    INTLINING, 
                    INVLEVEL, 
                    JTTYPE, 
                    LINKEDASSET, 
                    LOCALITY, 
                    MAKE, 
                    MEDIATYPE, 
                    MODBY, 
                    MODDTTM, 
                    MODELNO, 
                    PHOTO3D, 
                    PRESSURERATING, 
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
                    STIFFRATING, 
                    VARIATION_ID, 
                    WARRANTYEND, 
                    WARRANTYSTART, 
                    XSTORMSERVICELINEKEY, 
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
                    src.BUSINESSAREA, 
                    src.COMPKEY, 
                    src.CONFINEDSPACE, 
                    src.CONSTMETHOD, 
                    src.COSTCENTRE, 
                    src.CRITICALITY, 
                    src.DATALAKE_DELETED, 
                    src.DESIGNRESILIENCE, 
                    src.DIAMEXT, 
                    src.DIAMINT, 
                    src.DISPOSALDESC, 
                    src.DRAWINGNO, 
                    src.DRAWINGNOREF, 
                    src.EXTCOAT, 
                    src.FORDISPOSAL, 
                    src.FUNCAREA, 
                    src.GRDLEVEL, 
                    src.INTLINING, 
                    src.INVLEVEL, 
                    src.JTTYPE, 
                    src.LINKEDASSET, 
                    src.LOCALITY, 
                    src.MAKE, 
                    src.MEDIATYPE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MODELNO, 
                    src.PHOTO3D, 
                    src.PRESSURERATING, 
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
                    src.STIFFRATING, 
                    src.VARIATION_ID, 
                    src.WARRANTYEND, 
                    src.WARRANTYSTART, 
                    src.XSTORMSERVICELINEKEY, 
                    src.ZONECATCHMENT,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLASSETSSTORM_XSTORMSERVICELINE as target using (
                SELECT * FROM (SELECT 
            strm.ACCELDEPREC, 
            strm.ACCELDEPRECDESC, 
            strm.ACQTYPE, 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.BUSINESSAREA, 
            strm.COMPKEY, 
            strm.CONFINEDSPACE, 
            strm.CONSTMETHOD, 
            strm.COSTCENTRE, 
            strm.CRITICALITY, 
            strm.DATALAKE_DELETED, 
            strm.DESIGNRESILIENCE, 
            strm.DIAMEXT, 
            strm.DIAMINT, 
            strm.DISPOSALDESC, 
            strm.DRAWINGNO, 
            strm.DRAWINGNOREF, 
            strm.EXTCOAT, 
            strm.FORDISPOSAL, 
            strm.FUNCAREA, 
            strm.GRDLEVEL, 
            strm.INTLINING, 
            strm.INVLEVEL, 
            strm.JTTYPE, 
            strm.LINKEDASSET, 
            strm.LOCALITY, 
            strm.MAKE, 
            strm.MEDIATYPE, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.MODELNO, 
            strm.PHOTO3D, 
            strm.PRESSURERATING, 
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
            strm.STIFFRATING, 
            strm.VARIATION_ID, 
            strm.WARRANTYEND, 
            strm.WARRANTYSTART, 
            strm.XSTORMSERVICELINEKEY, 
            strm.ZONECATCHMENT, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XSTORMSERVICELINEKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLASSETSSTORM_XSTORMSERVICELINE as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XSTORMSERVICELINEKEY=src.XSTORMSERVICELINEKEY) OR (target.XSTORMSERVICELINEKEY IS NULL AND src.XSTORMSERVICELINEKEY IS NULL)) 
                when matched then update set
                    target.ACCELDEPREC=src.ACCELDEPREC, 
                    target.ACCELDEPRECDESC=src.ACCELDEPRECDESC, 
                    target.ACQTYPE=src.ACQTYPE, 
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.BUSINESSAREA=src.BUSINESSAREA, 
                    target.COMPKEY=src.COMPKEY, 
                    target.CONFINEDSPACE=src.CONFINEDSPACE, 
                    target.CONSTMETHOD=src.CONSTMETHOD, 
                    target.COSTCENTRE=src.COSTCENTRE, 
                    target.CRITICALITY=src.CRITICALITY, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DESIGNRESILIENCE=src.DESIGNRESILIENCE, 
                    target.DIAMEXT=src.DIAMEXT, 
                    target.DIAMINT=src.DIAMINT, 
                    target.DISPOSALDESC=src.DISPOSALDESC, 
                    target.DRAWINGNO=src.DRAWINGNO, 
                    target.DRAWINGNOREF=src.DRAWINGNOREF, 
                    target.EXTCOAT=src.EXTCOAT, 
                    target.FORDISPOSAL=src.FORDISPOSAL, 
                    target.FUNCAREA=src.FUNCAREA, 
                    target.GRDLEVEL=src.GRDLEVEL, 
                    target.INTLINING=src.INTLINING, 
                    target.INVLEVEL=src.INVLEVEL, 
                    target.JTTYPE=src.JTTYPE, 
                    target.LINKEDASSET=src.LINKEDASSET, 
                    target.LOCALITY=src.LOCALITY, 
                    target.MAKE=src.MAKE, 
                    target.MEDIATYPE=src.MEDIATYPE, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.MODELNO=src.MODELNO, 
                    target.PHOTO3D=src.PHOTO3D, 
                    target.PRESSURERATING=src.PRESSURERATING, 
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
                    target.STIFFRATING=src.STIFFRATING, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WARRANTYEND=src.WARRANTYEND, 
                    target.WARRANTYSTART=src.WARRANTYSTART, 
                    target.XSTORMSERVICELINEKEY=src.XSTORMSERVICELINEKEY, 
                    target.ZONECATCHMENT=src.ZONECATCHMENT, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ACCELDEPREC, ACCELDEPRECDESC, ACQTYPE, ADDBY, ADDDTTM, BUSINESSAREA, COMPKEY, CONFINEDSPACE, CONSTMETHOD, COSTCENTRE, CRITICALITY, DATALAKE_DELETED, DESIGNRESILIENCE, DIAMEXT, DIAMINT, DISPOSALDESC, DRAWINGNO, DRAWINGNOREF, EXTCOAT, FORDISPOSAL, FUNCAREA, GRDLEVEL, INTLINING, INVLEVEL, JTTYPE, LINKEDASSET, LOCALITY, MAKE, MEDIATYPE, MODBY, MODDTTM, MODELNO, PHOTO3D, PRESSURERATING, PROJECTNO, PROJECTWISE, QUAKEDESIGN, RELDATE, RELDEPTH, RELDIA, RELGEOSP, RELMATL, RELOWN, RELSTATUS, SAFETYCRITICAL, STIFFRATING, VARIATION_ID, WARRANTYEND, WARRANTYSTART, XSTORMSERVICELINEKEY, ZONECATCHMENT, 
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
                    src.BUSINESSAREA, 
                    src.COMPKEY, 
                    src.CONFINEDSPACE, 
                    src.CONSTMETHOD, 
                    src.COSTCENTRE, 
                    src.CRITICALITY, 
                    src.DATALAKE_DELETED, 
                    src.DESIGNRESILIENCE, 
                    src.DIAMEXT, 
                    src.DIAMINT, 
                    src.DISPOSALDESC, 
                    src.DRAWINGNO, 
                    src.DRAWINGNOREF, 
                    src.EXTCOAT, 
                    src.FORDISPOSAL, 
                    src.FUNCAREA, 
                    src.GRDLEVEL, 
                    src.INTLINING, 
                    src.INVLEVEL, 
                    src.JTTYPE, 
                    src.LINKEDASSET, 
                    src.LOCALITY, 
                    src.MAKE, 
                    src.MEDIATYPE, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.MODELNO, 
                    src.PHOTO3D, 
                    src.PRESSURERATING, 
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
                    src.STIFFRATING, 
                    src.VARIATION_ID, 
                    src.WARRANTYEND, 
                    src.WARRANTYSTART, 
                    src.XSTORMSERVICELINEKEY, 
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