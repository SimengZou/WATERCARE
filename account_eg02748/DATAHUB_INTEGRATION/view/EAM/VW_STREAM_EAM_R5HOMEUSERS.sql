CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5HOMEUSERS AS SELECT
                        src:HMU_AUTOFRESH::varchar AS HMU_AUTOFRESH, 
                        src:HMU_HOMCODE::varchar AS HMU_HOMCODE, 
                        src:HMU_HOMTYPE::varchar AS HMU_HOMTYPE, 
                        src:HMU_LASTSAVED::datetime AS HMU_LASTSAVED, 
                        src:HMU_SEQ::numeric(38, 10) AS HMU_SEQ, 
                        src:HMU_TAB::varchar AS HMU_TAB, 
                        src:HMU_UPDATECOUNT::numeric(38, 10) AS HMU_UPDATECOUNT, 
                        src:HMU_USER::varchar AS HMU_USER, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:HMU_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:HMU_HOMCODE,
                src:HMU_HOMTYPE,
                src:HMU_SEQ,
                src:HMU_USER  ORDER BY 
            src:HMU_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5HOMEUSERS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:HMU_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HMU_SEQ), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HMU_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:HMU_LASTSAVED), '1900-01-01')) is not null