CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_CDR_COMMONPLANCOPY AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:COMMONPLANCOPYKEY::integer AS COMMONPLANCOPYKEY, 
                        src:CONTACTKEY::integer AS CONTACTKEY, 
                        src:COPYID::varchar AS COPYID, 
                        src:COPYNO::integer AS COPYNO, 
                        src:COPYSTATUS::integer AS COPYSTATUS, 
                        src:COPYSTATUSDTTM::datetime AS COPYSTATUSDTTM, 
                        src:CURRENTLOC::varchar AS CURRENTLOC, 
                        src:CURRENTLOCDTTM::datetime AS CURRENTLOCDTTM, 
                        src:DELETED::boolean AS DELETED, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:OUTSOURCE::varchar AS OUTSOURCE, 
                        src:PLANCOPYTYPE::varchar AS PLANCOPYTYPE, 
                        src:REVIEWER::varchar AS REVIEWER, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:VERSION::varchar AS VERSION, 
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
    
                        
                src:COMMONPLANCOPYKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:COMMONPLANCOPYKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_CDR_COMMONPLANCOPY as strm))