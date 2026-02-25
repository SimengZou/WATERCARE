CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALERTWOFIELDMAPPING AS SELECT
                        src:AWF_ALERT::varchar AS AWF_ALERT, 
                        src:AWF_BOILERTEXTNUMBER::numeric(38, 10) AS AWF_BOILERTEXTNUMBER, 
                        src:AWF_GRIDFIELD::numeric(38, 10) AS AWF_GRIDFIELD, 
                        src:AWF_GRIDFIELDDATATYPE::varchar AS AWF_GRIDFIELDDATATYPE, 
                        src:AWF_LASTSAVED::datetime AS AWF_LASTSAVED, 
                        src:AWF_UPDATECOUNT::numeric(38, 10) AS AWF_UPDATECOUNT, 
                        src:AWF_WOFIELD::varchar AS AWF_WOFIELD, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:AWF_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:AWF_ALERT,
                src:AWF_WOFIELD  ORDER BY 
            src:AWF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTWOFIELDMAPPING as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AWF_BOILERTEXTNUMBER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AWF_GRIDFIELD), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AWF_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AWF_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AWF_LASTSAVED), '1900-01-01')) is not null