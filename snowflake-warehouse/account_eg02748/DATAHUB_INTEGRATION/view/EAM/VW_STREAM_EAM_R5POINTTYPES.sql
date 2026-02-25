CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5POINTTYPES AS SELECT
                        src:PTP_CLASS::varchar AS PTP_CLASS, 
                        src:PTP_CLASS_ORG::varchar AS PTP_CLASS_ORG, 
                        src:PTP_CODE::varchar AS PTP_CODE, 
                        src:PTP_CREATED::datetime AS PTP_CREATED, 
                        src:PTP_DESC::varchar AS PTP_DESC, 
                        src:PTP_LASTSAVED::datetime AS PTP_LASTSAVED, 
                        src:PTP_UPDATECOUNT::numeric(38, 10) AS PTP_UPDATECOUNT, 
                        src:PTP_UPDATED::datetime AS PTP_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PTP_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PTP_CODE  ORDER BY 
            src:PTP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5POINTTYPES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PTP_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PTP_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PTP_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PTP_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PTP_LASTSAVED), '1900-01-01')) is not null