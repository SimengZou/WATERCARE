create or replace procedure DATAHUB_INTEGRATION.SP_IPS_WSLCDRBUILD_XBUILDAPPCR()
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
                            SELECT * FROM LANDING_ERROR.VW_STREAM_IPS_WSLCDRBUILD_XBUILDAPPCR_ERROR`;
    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();
        
        var sql_command = ` 
                            merge into  DATAHUB_TARGET.IPS_WSLCDRBUILD_XBUILDAPPCR as target using (SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ALLOCATED, 
            strm.APBLDGKEY, 
            strm.APTYPE, 
            strm.CONTRACTOR, 
            strm.DELETED, 
            strm.DEVELOPERID, 
            strm.DEVELOPMENTAREA, 
            strm.FLOORAREA, 
            strm.GISLATERAL, 
            strm.IGCZONECODE, 
            strm.MASTERAPNO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.OWNERROLE, 
            strm.PARENTAPPLICATIONKEY, 
            strm.PARENTAPTYPE, 
            strm.RELOCATIONTYPE, 
            strm.RESUBMITWORKREQUEST, 
            strm.VARIATION_ID, 
            strm.XBUILDAPPCRKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XBUILDAPPCRKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRBUILD_XBUILDAPPCR as  strm
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XBUILDAPPCRKEY=src.XBUILDAPPCRKEY) OR (target.XBUILDAPPCRKEY IS NULL AND src.XBUILDAPPCRKEY IS NULL)) 
                when matched and src.ETL_DELETED=TRUE THEN DELETE
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ALLOCATED=src.ALLOCATED, 
                    target.APBLDGKEY=src.APBLDGKEY, 
                    target.APTYPE=src.APTYPE, 
                    target.CONTRACTOR=src.CONTRACTOR, 
                    target.DELETED=src.DELETED, 
                    target.DEVELOPERID=src.DEVELOPERID, 
                    target.DEVELOPMENTAREA=src.DEVELOPMENTAREA, 
                    target.FLOORAREA=src.FLOORAREA, 
                    target.GISLATERAL=src.GISLATERAL, 
                    target.IGCZONECODE=src.IGCZONECODE, 
                    target.MASTERAPNO=src.MASTERAPNO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.OWNERROLE=src.OWNERROLE, 
                    target.PARENTAPPLICATIONKEY=src.PARENTAPPLICATIONKEY, 
                    target.PARENTAPTYPE=src.PARENTAPTYPE, 
                    target.RELOCATIONTYPE=src.RELOCATIONTYPE, 
                    target.RESUBMITWORKREQUEST=src.RESUBMITWORKREQUEST, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.XBUILDAPPCRKEY=src.XBUILDAPPCRKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename
        when not matched and (src.ETL_DELETED=FALSE OR src.ETL_DELETED IS NULL)  then insert ( 
                    ADDBY, 
                    ADDDTTM, 
                    ALLOCATED, 
                    APBLDGKEY, 
                    APTYPE, 
                    CONTRACTOR, 
                    DELETED, 
                    DEVELOPERID, 
                    DEVELOPMENTAREA, 
                    FLOORAREA, 
                    GISLATERAL, 
                    IGCZONECODE, 
                    MASTERAPNO, 
                    MODBY, 
                    MODDTTM, 
                    OWNERROLE, 
                    PARENTAPPLICATIONKEY, 
                    PARENTAPTYPE, 
                    RELOCATIONTYPE, 
                    RESUBMITWORKREQUEST, 
                    VARIATION_ID, 
                    XBUILDAPPCRKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ALLOCATED, 
                    src.APBLDGKEY, 
                    src.APTYPE, 
                    src.CONTRACTOR, 
                    src.DELETED, 
                    src.DEVELOPERID, 
                    src.DEVELOPMENTAREA, 
                    src.FLOORAREA, 
                    src.GISLATERAL, 
                    src.IGCZONECODE, 
                    src.MASTERAPNO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.OWNERROLE, 
                    src.PARENTAPPLICATIONKEY, 
                    src.PARENTAPTYPE, 
                    src.RELOCATIONTYPE, 
                    src.RESUBMITWORKREQUEST, 
                    src.VARIATION_ID, 
                    src.XBUILDAPPCRKEY,     
                    src.ETL_SEQUENCE_NUMBER,
                    CURRENT_TIMESTAMP,
                    src.etl_load_metadatafilename);`;

    var sqlStmt = snowflake.createStatement( {sqlText:  sql_command} );
    var rs = sqlStmt.execute();

    snowflake.execute( {sqlText: `merge into DATAHUB_TARGET_HISTORY.IPS_DELETED_WSLCDRBUILD_XBUILDAPPCR as target using (
                SELECT * FROM (SELECT 
            strm.ADDBY, 
            strm.ADDDTTM, 
            strm.ALLOCATED, 
            strm.APBLDGKEY, 
            strm.APTYPE, 
            strm.CONTRACTOR, 
            strm.DELETED, 
            strm.DEVELOPERID, 
            strm.DEVELOPMENTAREA, 
            strm.FLOORAREA, 
            strm.GISLATERAL, 
            strm.IGCZONECODE, 
            strm.MASTERAPNO, 
            strm.MODBY, 
            strm.MODDTTM, 
            strm.OWNERROLE, 
            strm.PARENTAPPLICATIONKEY, 
            strm.PARENTAPTYPE, 
            strm.RELOCATIONTYPE, 
            strm.RESUBMITWORKREQUEST, 
            strm.VARIATION_ID, 
            strm.XBUILDAPPCRKEY, 
            strm.ETL_SEQUENCE_NUMBER,
            strm.ETL_DELETED,
            strm.etl_load_datetime,
            strm.etl_load_metadatafilename,
            ROW_NUMBER() OVER (PARTITION BY 
            strm.XBUILDAPPCRKEY ORDER BY strm.ETL_SEQUENCE_NUMBER desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
        FROM DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRBUILD_XBUILDAPPCR as  strm
        WHERE strm.ETL_DELETED=TRUE
                    )   
            WHERE ROWNUMBER=1  
            
                    ) as src on
            ((target.XBUILDAPPCRKEY=src.XBUILDAPPCRKEY) OR (target.XBUILDAPPCRKEY IS NULL AND src.XBUILDAPPCRKEY IS NULL)) 
                when matched then update set
                    target.ADDBY=src.ADDBY, 
                    target.ADDDTTM=src.ADDDTTM, 
                    target.ALLOCATED=src.ALLOCATED, 
                    target.APBLDGKEY=src.APBLDGKEY, 
                    target.APTYPE=src.APTYPE, 
                    target.CONTRACTOR=src.CONTRACTOR, 
                    target.DELETED=src.DELETED, 
                    target.DEVELOPERID=src.DEVELOPERID, 
                    target.DEVELOPMENTAREA=src.DEVELOPMENTAREA, 
                    target.FLOORAREA=src.FLOORAREA, 
                    target.GISLATERAL=src.GISLATERAL, 
                    target.IGCZONECODE=src.IGCZONECODE, 
                    target.MASTERAPNO=src.MASTERAPNO, 
                    target.MODBY=src.MODBY, 
                    target.MODDTTM=src.MODDTTM, 
                    target.OWNERROLE=src.OWNERROLE, 
                    target.PARENTAPPLICATIONKEY=src.PARENTAPPLICATIONKEY, 
                    target.PARENTAPTYPE=src.PARENTAPTYPE, 
                    target.RELOCATIONTYPE=src.RELOCATIONTYPE, 
                    target.RESUBMITWORKREQUEST=src.RESUBMITWORKREQUEST, 
                    target.VARIATION_ID=src.VARIATION_ID, 
                    target.XBUILDAPPCRKEY=src.XBUILDAPPCRKEY, 
                    target.ETL_SEQUENCE_NUMBER=src.ETL_SEQUENCE_NUMBER,
                    target.etl_load_datetime=CURRENT_TIMESTAMP,
                    target.etl_load_metadatafilename=src.etl_load_metadatafilename,
                    target.ETL_DELETED=src.ETL_DELETED
        when not matched   then insert ( ADDBY, ADDDTTM, ALLOCATED, APBLDGKEY, APTYPE, CONTRACTOR, DELETED, DEVELOPERID, DEVELOPMENTAREA, FLOORAREA, GISLATERAL, IGCZONECODE, MASTERAPNO, MODBY, MODDTTM, OWNERROLE, PARENTAPPLICATIONKEY, PARENTAPTYPE, RELOCATIONTYPE, RESUBMITWORKREQUEST, VARIATION_ID, XBUILDAPPCRKEY, 
                    ETL_SEQUENCE_NUMBER,                       
                    etl_load_datetime,
                    etl_load_metadatafilename,
                    etl_is_deleted,
                    ETL_DELETED) 
            values (
                    src.ADDBY, 
                    src.ADDDTTM, 
                    src.ALLOCATED, 
                    src.APBLDGKEY, 
                    src.APTYPE, 
                    src.CONTRACTOR, 
                    src.DELETED, 
                    src.DEVELOPERID, 
                    src.DEVELOPMENTAREA, 
                    src.FLOORAREA, 
                    src.GISLATERAL, 
                    src.IGCZONECODE, 
                    src.MASTERAPNO, 
                    src.MODBY, 
                    src.MODDTTM, 
                    src.OWNERROLE, 
                    src.PARENTAPPLICATIONKEY, 
                    src.PARENTAPTYPE, 
                    src.RELOCATIONTYPE, 
                    src.RESUBMITWORKREQUEST, 
                    src.VARIATION_ID, 
                    src.XBUILDAPPCRKEY,     
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