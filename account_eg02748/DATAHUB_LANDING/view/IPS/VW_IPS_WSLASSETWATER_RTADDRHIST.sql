CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLASSETWATER_RTADDRHIST AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRKEY::integer AS ADDRKEY, 
                        src:DELETED::boolean AS DELETED, 
                        src:INDEXNO::integer AS INDEXNO, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:ORIGADDBY::varchar AS ORIGADDBY, 
                        src:ORIGADDDTTM::datetime AS ORIGADDDTTM, 
                        src:ORIGMODBY::varchar AS ORIGMODBY, 
                        src:ORIGMODDTTM::datetime AS ORIGMODDTTM, 
                        src:POSITION::integer AS POSITION, 
                        src:ROUTEKEY::integer AS ROUTEKEY, 
                        src:RTADDRHISTKEY::integer AS RTADDRHISTKEY, 
                        src:RTADDRKEY::integer AS RTADDRKEY, 
                        src:SEQNO::integer AS SEQNO, 
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
    
                        
                src:RTADDRHISTKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:RTADDRHISTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLASSETWATER_RTADDRHIST as strm))