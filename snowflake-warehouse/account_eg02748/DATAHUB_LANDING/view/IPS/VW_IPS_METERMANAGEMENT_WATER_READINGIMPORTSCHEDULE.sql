CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_METERMANAGEMENT_WATER_READINGIMPORTSCHEDULE AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CYCLEBASEDFLAG::varchar AS CYCLEBASEDFLAG, 
                        src:DELETED::boolean AS DELETED, 
                        src:IMPEXPDAYS::integer AS IMPEXPDAYS, 
                        src:IMPORTDIRPATH::varchar AS IMPORTDIRPATH, 
                        src:IMPORTFILETEMPLATE::varchar AS IMPORTFILETEMPLATE, 
                        src:IMPORTRUNNUMBER::integer AS IMPORTRUNNUMBER, 
                        src:IMPORTTYPE::integer AS IMPORTTYPE, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PROCESSREADYTOBILLFLAG::varchar AS PROCESSREADYTOBILLFLAG, 
                        src:READINGEXPORTSCHEDULEKEY::integer AS READINGEXPORTSCHEDULEKEY, 
                        src:READINGIMPORTSCHEDULEKEY::integer AS READINGIMPORTSCHEDULEKEY, 
                        src:RECEIVERCODE::varchar AS RECEIVERCODE, 
                        src:REJECTNONEXROUTE::varchar AS REJECTNONEXROUTE, 
                        src:REJECTPRIORMETERREAD::varchar AS REJECTPRIORMETERREAD, 
                        src:RERUNFLAG::varchar AS RERUNFLAG, 
                        src:RERUNNUMBER::integer AS RERUNNUMBER, 
                        src:RESOLVESRFLAG::varchar AS RESOLVESRFLAG, 
                        src:RUNNUMBER::integer AS RUNNUMBER, 
                        src:SCHEDULEKEY::integer AS SCHEDULEKEY, 
                        src:SELFIMPORTFLAG::varchar AS SELFIMPORTFLAG, 
                        src:SENDERCODE::varchar AS SENDERCODE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:READINGIMPORTSCHEDULEKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:READINGIMPORTSCHEDULEKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_METERMANAGEMENT_WATER_READINGIMPORTSCHEDULE as strm))