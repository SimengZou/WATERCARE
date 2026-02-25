create or replace procedure DATAHUB_INTEGRATION.SP_IPS_PROPERTY_PARCEL()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_PROPERTY_PARCEL_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_PROPERTY_PARCEL as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AREA, 
            strm.ATLASPAGE, 
            strm.BLOCK, 
            strm.COMMELEM, 
            strm.COUNTY, 
            strm.CURRPRCL, 
            strm.DELETED, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.LEGALOWNER, 
            strm.LNDPRICETY, 
            strm.LOT, 
            strm.LOTDPTH, 
            strm.LOTFRONTAG, 
            strm.LOTSHP, 
            strm.LOTSZ, 
            strm.LOTUM, 
            strm.MAPNO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PLANMEDIA, 
            strm.PRCLID, 
            strm.PRCLKEY, 
            strm.PRCLNAME, 
            strm.PRCLSTAT, 
            strm.PRCLTYPE, 
            strm.PROPNOTE, 
            strm.RANGE, 
            strm.SECTION, 
            strm.SUBDIV, 
            strm.SUBDIVCODE, 
            strm.SUBOFKEY, 
            strm.TOWNSHIP, 
            strm.VARIATION_ID, 
            strm.VERSION, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PRCLKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_PROPERTY_PARCEL as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PRCLKEY=src.PRCLKEY) OR (target.PRCLKEY IS NULL AND src.PRCLKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AREA=src.AREA, 
                    target.ATLASPAGE=src.ATLASPAGE, 
                    target.BLOCK=src.BLOCK, 
                    target.COMMELEM=src.COMMELEM, 
                    target.COUNTY=src.COUNTY, 
                    target.CURRPRCL=src.CURRPRCL, 
                    target.DELETED=src.DELETED, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.LEGALOWNER=src.LEGALOWNER, 
                    target.LNDPRICETY=src.LNDPRICETY, 
                    target.LOT=src.LOT, 
                    target.LOTDPTH=src.LOTDPTH, 
                    target.LOTFRONTAG=src.LOTFRONTAG, 
                    target.LOTSHP=src.LOTSHP, 
                    target.LOTSZ=src.LOTSZ, 
                    target.LOTUM=src.LOTUM, 
                    target.MAPNO=src.MAPNO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PLANMEDIA=src.PLANMEDIA, 
                    target.PRCLID=src.PRCLID, 
                    target.PRCLKEY=src.PRCLKEY, 
                    target.PRCLNAME=src.PRCLNAME, 
                    target.PRCLSTAT=src.PRCLSTAT, 
                    target.PRCLTYPE=src.PRCLTYPE, 
                    target.PROPNOTE=src.PROPNOTE, 
                    target.RANGE=src.RANGE, 
                    target.SECTION=src.SECTION, 
                    target.SUBDIV=src.SUBDIV, 
                    target.SUBDIVCODE=src.SUBDIVCODE, 
                    target.SUBOFKEY=src.SUBOFKEY, 
                    target.TOWNSHIP=src.TOWNSHIP, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VERSION=src.VERSION, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    AREA, 
                    ATLASPAGE, 
                    BLOCK, 
                    COMMELEM, 
                    COUNTY, 
                    CURRPRCL, 
                    DELETED, 
                    EFFDATE, 
                    EXPDATE, 
                    LEGALOWNER, 
                    LNDPRICETY, 
                    LOT, 
                    LOTDPTH, 
                    LOTFRONTAG, 
                    LOTSHP, 
                    LOTSZ, 
                    LOTUM, 
                    MAPNO, 
                    MODBY, 
                    MODDTTM, 
                    PLANMEDIA, 
                    PRCLID, 
                    PRCLKEY, 
                    PRCLNAME, 
                    PRCLSTAT, 
                    PRCLTYPE, 
                    PROPNOTE, 
                    RANGE, 
                    SECTION, 
                    SUBDIV, 
                    SUBDIVCODE, 
                    SUBOFKEY, 
                    TOWNSHIP, 
                    VARIATION_ID, 
                    VERSION, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AREA, 
                    src.ATLASPAGE, 
                    src.BLOCK, 
                    src.COMMELEM, 
                    src.COUNTY, 
                    src.CURRPRCL, 
                    src.DELETED, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.LEGALOWNER, 
                    src.LNDPRICETY, 
                    src.LOT, 
                    src.LOTDPTH, 
                    src.LOTFRONTAG, 
                    src.LOTSHP, 
                    src.LOTSZ, 
                    src.LOTUM, 
                    src.MAPNO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PLANMEDIA, 
                    src.PRCLID, 
                    src.PRCLKEY, 
                    src.PRCLNAME, 
                    src.PRCLSTAT, 
                    src.PRCLTYPE, 
                    src.PROPNOTE, 
                    src.RANGE, 
                    src.SECTION, 
                    src.SUBDIV, 
                    src.SUBDIVCODE, 
                    src.SUBOFKEY, 
                    src.TOWNSHIP, 
                    src.VARIATION_ID, 
                    src.VERSION,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_PROPERTY_PARCEL as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.AREA, 
            strm.ATLASPAGE, 
            strm.BLOCK, 
            strm.COMMELEM, 
            strm.COUNTY, 
            strm.CURRPRCL, 
            strm.DELETED, 
            strm.EFFDATE, 
            strm.EXPDATE, 
            strm.LEGALOWNER, 
            strm.LNDPRICETY, 
            strm.LOT, 
            strm.LOTDPTH, 
            strm.LOTFRONTAG, 
            strm.LOTSHP, 
            strm.LOTSZ, 
            strm.LOTUM, 
            strm.MAPNO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.PLANMEDIA, 
            strm.PRCLID, 
            strm.PRCLKEY, 
            strm.PRCLNAME, 
            strm.PRCLSTAT, 
            strm.PRCLTYPE, 
            strm.PROPNOTE, 
            strm.RANGE, 
            strm.SECTION, 
            strm.SUBDIV, 
            strm.SUBDIVCODE, 
            strm.SUBOFKEY, 
            strm.TOWNSHIP, 
            strm.VARIATION_ID, 
            strm.VERSION, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.PRCLKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_PROPERTY_PARCEL as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.PRCLKEY=src.PRCLKEY) OR (target.PRCLKEY IS NULL AND src.PRCLKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.AREA=src.AREA, 
                    target.ATLASPAGE=src.ATLASPAGE, 
                    target.BLOCK=src.BLOCK, 
                    target.COMMELEM=src.COMMELEM, 
                    target.COUNTY=src.COUNTY, 
                    target.CURRPRCL=src.CURRPRCL, 
                    target.DELETED=src.DELETED, 
                    target.EFFDATE=src.EFFDATE, 
                    target.EXPDATE=src.EXPDATE, 
                    target.LEGALOWNER=src.LEGALOWNER, 
                    target.LNDPRICETY=src.LNDPRICETY, 
                    target.LOT=src.LOT, 
                    target.LOTDPTH=src.LOTDPTH, 
                    target.LOTFRONTAG=src.LOTFRONTAG, 
                    target.LOTSHP=src.LOTSHP, 
                    target.LOTSZ=src.LOTSZ, 
                    target.LOTUM=src.LOTUM, 
                    target.MAPNO=src.MAPNO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.PLANMEDIA=src.PLANMEDIA, 
                    target.PRCLID=src.PRCLID, 
                    target.PRCLKEY=src.PRCLKEY, 
                    target.PRCLNAME=src.PRCLNAME, 
                    target.PRCLSTAT=src.PRCLSTAT, 
                    target.PRCLTYPE=src.PRCLTYPE, 
                    target.PROPNOTE=src.PROPNOTE, 
                    target.RANGE=src.RANGE, 
                    target.SECTION=src.SECTION, 
                    target.SUBDIV=src.SUBDIV, 
                    target.SUBDIVCODE=src.SUBDIVCODE, 
                    target.SUBOFKEY=src.SUBOFKEY, 
                    target.TOWNSHIP=src.TOWNSHIP, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.VERSION=src.VERSION, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, AREA, ATLASPAGE, BLOCK, COMMELEM, COUNTY, CURRPRCL, DELETED, EFFDATE, EXPDATE, LEGALOWNER, LNDPRICETY, LOT, LOTDPTH, LOTFRONTAG, LOTSHP, LOTSZ, LOTUM, MAPNO, MODBY, MODDTTM, PLANMEDIA, PRCLID, PRCLKEY, PRCLNAME, PRCLSTAT, PRCLTYPE, PROPNOTE, RANGE, SECTION, SUBDIV, SUBDIVCODE, SUBOFKEY, TOWNSHIP, VARIATION_ID, VERSION, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.AREA, 
                    src.ATLASPAGE, 
                    src.BLOCK, 
                    src.COMMELEM, 
                    src.COUNTY, 
                    src.CURRPRCL, 
                    src.DELETED, 
                    src.EFFDATE, 
                    src.EXPDATE, 
                    src.LEGALOWNER, 
                    src.LNDPRICETY, 
                    src.LOT, 
                    src.LOTDPTH, 
                    src.LOTFRONTAG, 
                    src.LOTSHP, 
                    src.LOTSZ, 
                    src.LOTUM, 
                    src.MAPNO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.PLANMEDIA, 
                    src.PRCLID, 
                    src.PRCLKEY, 
                    src.PRCLNAME, 
                    src.PRCLSTAT, 
                    src.PRCLTYPE, 
                    src.PROPNOTE, 
                    src.RANGE, 
                    src.SECTION, 
                    src.SUBDIV, 
                    src.SUBDIVCODE, 
                    src.SUBOFKEY, 
                    src.TOWNSHIP, 
                    src.VARIATION_ID, 
                    src.VERSION,     
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