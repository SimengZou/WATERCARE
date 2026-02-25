CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5EDITIONBACKREPS AS SELECT
                        src:EBR_FUNCTION::varchar AS EBR_FUNCTION, 
                        src:EBR_LASTSAVED::datetime AS EBR_LASTSAVED, 
                        src:EBR_MEFLAG::varchar AS EBR_MEFLAG, 
                        src:EBR_REPORT::varchar AS EBR_REPORT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:EBR_LASTSAVED::datetime as ETL_LASTSAVED,
            CASE
                WHEN 'EAM' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'EAM' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED, 
            etl_load_datetime,
            etl_load_metadatafilename
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename
                from
                (
                    SELECT
                        
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:EBR_FUNCTION,
                src:EBR_MEFLAG  ORDER BY 
            src:EBR_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5EDITIONBACKREPS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EBR_LASTSAVED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:EBR_LASTSAVED), '1900-01-01')) is not null