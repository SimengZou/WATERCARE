CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_U5CONTPROJACTIVITIES AS SELECT
                        src:CREATED::datetime AS CREATED, 
                        src:CREATEDBY::varchar AS CREATEDBY, 
                        src:CSA_ACTIVITY::varchar AS CSA_ACTIVITY, 
                        src:CSA_CONTRACT::varchar AS CSA_CONTRACT, 
                        src:CSA_CONTRACTCODE::varchar AS CSA_CONTRACTCODE, 
                        src:CSA_CONTRACTOR::varchar AS CSA_CONTRACTOR, 
                        src:CSA_ORG::varchar AS CSA_ORG, 
                        src:CSA_PROJECT::varchar AS CSA_PROJECT, 
                        src:CSA_SCHEDULEITEM::varchar AS CSA_SCHEDULEITEM, 
                        src:CSA_SUPPLIER::varchar AS CSA_SUPPLIER, 
                        src:LASTSAVED::datetime AS LASTSAVED, 
                        src:UPDATECOUNT::numeric(38, 10) AS UPDATECOUNT, 
                        src:UPDATED::datetime AS UPDATED, 
                        src:UPDATEDBY::varchar AS UPDATEDBY, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:CSA_CONTRACT,
                src:CSA_ORG,
                src:CSA_SCHEDULEITEM  ORDER BY 
            src:LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_U5CONTPROJACTIVITIES as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:LASTSAVED), '1900-01-01')) is not null