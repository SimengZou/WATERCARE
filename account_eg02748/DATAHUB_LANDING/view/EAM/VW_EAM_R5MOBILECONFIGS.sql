CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5MOBILECONFIGS AS SELECT
                        src:MCF_CODE::varchar AS MCF_CODE, 
                        src:MCF_COMPTYPE::varchar AS MCF_COMPTYPE, 
                        src:MCF_CONFIG::numeric(38, 10) AS MCF_CONFIG, 
                        src:MCF_CREATED::datetime AS MCF_CREATED, 
                        src:MCF_DESK::varchar AS MCF_DESK, 
                        src:MCF_GROUP::varchar AS MCF_GROUP, 
                        src:MCF_LASTSAVED::datetime AS MCF_LASTSAVED, 
                        src:MCF_RCODE::varchar AS MCF_RCODE, 
                        src:MCF_UPDATECOUNT::numeric(38, 10) AS MCF_UPDATECOUNT, 
                        src:MCF_UPDATED::datetime AS MCF_UPDATED, 
                        src:MCF_USER::varchar AS MCF_USER, 
                        src:MCF_XMLCONFIG::varchar AS MCF_XMLCONFIG, 
                        src:_DELETED::boolean AS _DELETED, 
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
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
    
                        
                src:MCF_CODE,
                src:MCF_CONFIG,
                src:MCF_DESK,
                src:MCF_RCODE,
            src:MCF_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:MCF_CODE,
                src:MCF_CONFIG,
                src:MCF_DESK,
                src:MCF_RCODE  ORDER BY 
            src:MCF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5MOBILECONFIGS as strm))