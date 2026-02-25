CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5PPMPLAN AS SELECT
                        src:PMP_CLASS::varchar AS PMP_CLASS, 
                        src:PMP_CLASS_ORG::varchar AS PMP_CLASS_ORG, 
                        src:PMP_CODE::varchar AS PMP_CODE, 
                        src:PMP_DESC::varchar AS PMP_DESC, 
                        src:PMP_LASTSAVED::datetime AS PMP_LASTSAVED, 
                        src:PMP_OBJECTCLASS::varchar AS PMP_OBJECTCLASS, 
                        src:PMP_OBJECTCLASS_ORG::varchar AS PMP_OBJECTCLASS_ORG, 
                        src:PMP_ORG::varchar AS PMP_ORG, 
                        src:PMP_UPDATECOUNT::numeric(38, 10) AS PMP_UPDATECOUNT, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:PMP_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:PMP_CODE  ORDER BY 
            src:PMP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5PPMPLAN as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PMP_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PMP_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PMP_LASTSAVED), '1900-01-01')) is not null