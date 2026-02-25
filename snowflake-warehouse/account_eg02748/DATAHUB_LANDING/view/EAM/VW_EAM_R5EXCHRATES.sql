CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5EXCHRATES AS SELECT
                        src:CRR_ACTIVE::varchar AS CRR_ACTIVE, 
                        src:CRR_CODE::varchar AS CRR_CODE, 
                        src:CRR_CURR::varchar AS CRR_CURR, 
                        src:CRR_END::datetime AS CRR_END, 
                        src:CRR_EXCH::numeric(38, 10) AS CRR_EXCH, 
                        src:CRR_INTERFACE::datetime AS CRR_INTERFACE, 
                        src:CRR_LASTSAVED::datetime AS CRR_LASTSAVED, 
                        src:CRR_ORGCURR::varchar AS CRR_ORGCURR, 
                        src:CRR_SOURCECODE::varchar AS CRR_SOURCECODE, 
                        src:CRR_SOURCESYSTEM::varchar AS CRR_SOURCESYSTEM, 
                        src:CRR_START::datetime AS CRR_START, 
                        src:CRR_UPDATECOUNT::numeric(38, 10) AS CRR_UPDATECOUNT, 
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
    
                        
                src:CRR_CODE,
            src:CRR_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:CRR_CODE  ORDER BY 
            src:CRR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5EXCHRATES as strm))