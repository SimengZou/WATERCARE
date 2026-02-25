CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_EAM_R5INSTALL AS SELECT
                        src:INS_CODE::varchar AS INS_CODE, 
                        src:INS_COMMENT::varchar AS INS_COMMENT, 
                        src:INS_DESC::varchar AS INS_DESC, 
                        src:INS_EDCOMMENT::varchar AS INS_EDCOMMENT, 
                        src:INS_EXTENDED::varchar AS INS_EXTENDED, 
                        src:INS_FIXED::varchar AS INS_FIXED, 
                        src:INS_LASTSAVED::datetime AS INS_LASTSAVED, 
                        src:INS_MODULE::varchar AS INS_MODULE, 
                        src:INS_UPDATECOUNT::numeric(38, 10) AS INS_UPDATECOUNT, 
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
    
                        
                src:INS_CODE,
            src:INS_LASTSAVED
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:INS_CODE  ORDER BY 
            src:INS_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.EAM_R5INSTALL as strm))