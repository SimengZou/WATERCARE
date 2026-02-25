CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_METERMANAGEMENT_WATER_CYCLEROUTEAVGCONSUMPTIONRUN AS SELECT
                        src:ACCOUNTCLASS::varchar AS ACCOUNTCLASS, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:CYCLEROUTEAVGCONSKEY::integer AS CYCLEROUTEAVGCONSKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PERIODFROM::datetime AS PERIODFROM, 
                        src:PERIODTO::datetime AS PERIODTO, 
                        src:PROCESSINGFLAG::varchar AS PROCESSINGFLAG, 
                        src:READINGCYCLE::varchar AS READINGCYCLE, 
                        src:SCHEDULEKEY::integer AS SCHEDULEKEY, 
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
    
                        
                src:CYCLEROUTEAVGCONSKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CYCLEROUTEAVGCONSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_METERMANAGEMENT_WATER_CYCLEROUTEAVGCONSUMPTIONRUN as strm))