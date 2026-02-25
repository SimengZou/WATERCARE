create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLINTERFACE_NONDOMMULTIPLESERVICES()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLINTERFACE_NONDOMMULTIPLESERVICES_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLINTERFACE_NONDOMMULTIPLESERVICES as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APBLDGINSPKEY, 
            strm.ARBORIST, 
            strm.BACKFLOWREQUIRED, 
            strm.BACKFLOWSTRAINER, 
            strm.BACKFLOWTYPE, 
            strm.COMPKEY, 
            strm.CRITICALLINES, 
            strm.CUSTOMERTMP, 
            strm.DATALAKE_DELETED, 
            strm.DRAWINGNO, 
            strm.INSPECTIONTYPE, 
            strm.MAGFLOWPOWERTYPE, 
            strm.MAGFLOWREQUIRED, 
            strm.METERBOXSIZE, 
            strm.METERBOXTYPE, 
            strm.METERID, 
            strm.METERLOCATION, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NONDOMESTICCONSTAGINGKEY, 
            strm.NONDOMMULTIPLESERVICESKEY, 
            strm.PLANSATTACHED, 
            strm.PROPOSEDLOCATION, 
            strm.REMOTEREADER, 
            strm.SIZEMETER, 
            strm.SPRINKLERSUPPLYPIPESIZE, 
            strm.VARIATION_ID, 
            strm.WATERMAINLOCATION, 
            strm.WATERMAINMATERIAL, 
            strm.WATERMAINSIZE, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.NONDOMMULTIPLESERVICESKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_NONDOMMULTIPLESERVICES as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.NONDOMMULTIPLESERVICESKEY=src.NONDOMMULTIPLESERVICESKEY) OR (target.NONDOMMULTIPLESERVICESKEY IS NULL AND src.NONDOMMULTIPLESERVICESKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APBLDGINSPKEY=src.APBLDGINSPKEY, 
                    target.ARBORIST=src.ARBORIST, 
                    target.BACKFLOWREQUIRED=src.BACKFLOWREQUIRED, 
                    target.BACKFLOWSTRAINER=src.BACKFLOWSTRAINER, 
                    target.BACKFLOWTYPE=src.BACKFLOWTYPE, 
                    target.COMPKEY=src.COMPKEY, 
                    target.CRITICALLINES=src.CRITICALLINES, 
                    target.CUSTOMERTMP=src.CUSTOMERTMP, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DRAWINGNO=src.DRAWINGNO, 
                    target.INSPECTIONTYPE=src.INSPECTIONTYPE, 
                    target.MAGFLOWPOWERTYPE=src.MAGFLOWPOWERTYPE, 
                    target.MAGFLOWREQUIRED=src.MAGFLOWREQUIRED, 
                    target.METERBOXSIZE=src.METERBOXSIZE, 
                    target.METERBOXTYPE=src.METERBOXTYPE, 
                    target.METERID=src.METERID, 
                    target.METERLOCATION=src.METERLOCATION, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NONDOMESTICCONSTAGINGKEY=src.NONDOMESTICCONSTAGINGKEY, 
                    target.NONDOMMULTIPLESERVICESKEY=src.NONDOMMULTIPLESERVICESKEY, 
                    target.PLANSATTACHED=src.PLANSATTACHED, 
                    target.PROPOSEDLOCATION=src.PROPOSEDLOCATION, 
                    target.REMOTEREADER=src.REMOTEREADER, 
                    target.SIZEMETER=src.SIZEMETER, 
                    target.SPRINKLERSUPPLYPIPESIZE=src.SPRINKLERSUPPLYPIPESIZE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WATERMAINLOCATION=src.WATERMAINLOCATION, 
                    target.WATERMAINMATERIAL=src.WATERMAINMATERIAL, 
                    target.WATERMAINSIZE=src.WATERMAINSIZE, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    APBLDGINSPKEY, 
                    ARBORIST, 
                    BACKFLOWREQUIRED, 
                    BACKFLOWSTRAINER, 
                    BACKFLOWTYPE, 
                    COMPKEY, 
                    CRITICALLINES, 
                    CUSTOMERTMP, 
                    DATALAKE_DELETED, 
                    DRAWINGNO, 
                    INSPECTIONTYPE, 
                    MAGFLOWPOWERTYPE, 
                    MAGFLOWREQUIRED, 
                    METERBOXSIZE, 
                    METERBOXTYPE, 
                    METERID, 
                    METERLOCATION, 
                    MODBY, 
                    MODDTTM, 
                    NONDOMESTICCONSTAGINGKEY, 
                    NONDOMMULTIPLESERVICESKEY, 
                    PLANSATTACHED, 
                    PROPOSEDLOCATION, 
                    REMOTEREADER, 
                    SIZEMETER, 
                    SPRINKLERSUPPLYPIPESIZE, 
                    VARIATION_ID, 
                    WATERMAINLOCATION, 
                    WATERMAINMATERIAL, 
                    WATERMAINSIZE, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APBLDGINSPKEY, 
                    src.ARBORIST, 
                    src.BACKFLOWREQUIRED, 
                    src.BACKFLOWSTRAINER, 
                    src.BACKFLOWTYPE, 
                    src.COMPKEY, 
                    src.CRITICALLINES, 
                    src.CUSTOMERTMP, 
                    src.DATALAKE_DELETED, 
                    src.DRAWINGNO, 
                    src.INSPECTIONTYPE, 
                    src.MAGFLOWPOWERTYPE, 
                    src.MAGFLOWREQUIRED, 
                    src.METERBOXSIZE, 
                    src.METERBOXTYPE, 
                    src.METERID, 
                    src.METERLOCATION, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NONDOMESTICCONSTAGINGKEY, 
                    src.NONDOMMULTIPLESERVICESKEY, 
                    src.PLANSATTACHED, 
                    src.PROPOSEDLOCATION, 
                    src.REMOTEREADER, 
                    src.SIZEMETER, 
                    src.SPRINKLERSUPPLYPIPESIZE, 
                    src.VARIATION_ID, 
                    src.WATERMAINLOCATION, 
                    src.WATERMAINMATERIAL, 
                    src.WATERMAINSIZE,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLINTERFACE_NONDOMMULTIPLESERVICES as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.APBLDGINSPKEY, 
            strm.ARBORIST, 
            strm.BACKFLOWREQUIRED, 
            strm.BACKFLOWSTRAINER, 
            strm.BACKFLOWTYPE, 
            strm.COMPKEY, 
            strm.CRITICALLINES, 
            strm.CUSTOMERTMP, 
            strm.DATALAKE_DELETED, 
            strm.DRAWINGNO, 
            strm.INSPECTIONTYPE, 
            strm.MAGFLOWPOWERTYPE, 
            strm.MAGFLOWREQUIRED, 
            strm.METERBOXSIZE, 
            strm.METERBOXTYPE, 
            strm.METERID, 
            strm.METERLOCATION, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.NONDOMESTICCONSTAGINGKEY, 
            strm.NONDOMMULTIPLESERVICESKEY, 
            strm.PLANSATTACHED, 
            strm.PROPOSEDLOCATION, 
            strm.REMOTEREADER, 
            strm.SIZEMETER, 
            strm.SPRINKLERSUPPLYPIPESIZE, 
            strm.VARIATION_ID, 
            strm.WATERMAINLOCATION, 
            strm.WATERMAINMATERIAL, 
            strm.WATERMAINSIZE, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.NONDOMMULTIPLESERVICESKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLINTERFACE_NONDOMMULTIPLESERVICES as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.NONDOMMULTIPLESERVICESKEY=src.NONDOMMULTIPLESERVICESKEY) OR (target.NONDOMMULTIPLESERVICESKEY IS NULL AND src.NONDOMMULTIPLESERVICESKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.APBLDGINSPKEY=src.APBLDGINSPKEY, 
                    target.ARBORIST=src.ARBORIST, 
                    target.BACKFLOWREQUIRED=src.BACKFLOWREQUIRED, 
                    target.BACKFLOWSTRAINER=src.BACKFLOWSTRAINER, 
                    target.BACKFLOWTYPE=src.BACKFLOWTYPE, 
                    target.COMPKEY=src.COMPKEY, 
                    target.CRITICALLINES=src.CRITICALLINES, 
                    target.CUSTOMERTMP=src.CUSTOMERTMP, 
                    target.DATALAKE_DELETED=src.DATALAKE_DELETED, 
                    target.DRAWINGNO=src.DRAWINGNO, 
                    target.INSPECTIONTYPE=src.INSPECTIONTYPE, 
                    target.MAGFLOWPOWERTYPE=src.MAGFLOWPOWERTYPE, 
                    target.MAGFLOWREQUIRED=src.MAGFLOWREQUIRED, 
                    target.METERBOXSIZE=src.METERBOXSIZE, 
                    target.METERBOXTYPE=src.METERBOXTYPE, 
                    target.METERID=src.METERID, 
                    target.METERLOCATION=src.METERLOCATION, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.NONDOMESTICCONSTAGINGKEY=src.NONDOMESTICCONSTAGINGKEY, 
                    target.NONDOMMULTIPLESERVICESKEY=src.NONDOMMULTIPLESERVICESKEY, 
                    target.PLANSATTACHED=src.PLANSATTACHED, 
                    target.PROPOSEDLOCATION=src.PROPOSEDLOCATION, 
                    target.REMOTEREADER=src.REMOTEREADER, 
                    target.SIZEMETER=src.SIZEMETER, 
                    target.SPRINKLERSUPPLYPIPESIZE=src.SPRINKLERSUPPLYPIPESIZE, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.WATERMAINLOCATION=src.WATERMAINLOCATION, 
                    target.WATERMAINMATERIAL=src.WATERMAINMATERIAL, 
                    target.WATERMAINSIZE=src.WATERMAINSIZE, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, APBLDGINSPKEY, ARBORIST, BACKFLOWREQUIRED, BACKFLOWSTRAINER, BACKFLOWTYPE, COMPKEY, CRITICALLINES, CUSTOMERTMP, DATALAKE_DELETED, DRAWINGNO, INSPECTIONTYPE, MAGFLOWPOWERTYPE, MAGFLOWREQUIRED, METERBOXSIZE, METERBOXTYPE, METERID, METERLOCATION, MODBY, MODDTTM, NONDOMESTICCONSTAGINGKEY, NONDOMMULTIPLESERVICESKEY, PLANSATTACHED, PROPOSEDLOCATION, REMOTEREADER, SIZEMETER, SPRINKLERSUPPLYPIPESIZE, VARIATION_ID, WATERMAINLOCATION, WATERMAINMATERIAL, WATERMAINSIZE, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.APBLDGINSPKEY, 
                    src.ARBORIST, 
                    src.BACKFLOWREQUIRED, 
                    src.BACKFLOWSTRAINER, 
                    src.BACKFLOWTYPE, 
                    src.COMPKEY, 
                    src.CRITICALLINES, 
                    src.CUSTOMERTMP, 
                    src.DATALAKE_DELETED, 
                    src.DRAWINGNO, 
                    src.INSPECTIONTYPE, 
                    src.MAGFLOWPOWERTYPE, 
                    src.MAGFLOWREQUIRED, 
                    src.METERBOXSIZE, 
                    src.METERBOXTYPE, 
                    src.METERID, 
                    src.METERLOCATION, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.NONDOMESTICCONSTAGINGKEY, 
                    src.NONDOMMULTIPLESERVICESKEY, 
                    src.PLANSATTACHED, 
                    src.PROPOSEDLOCATION, 
                    src.REMOTEREADER, 
                    src.SIZEMETER, 
                    src.SPRINKLERSUPPLYPIPESIZE, 
                    src.VARIATION_ID, 
                    src.WATERMAINLOCATION, 
                    src.WATERMAINMATERIAL, 
                    src.WATERMAINSIZE,     
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