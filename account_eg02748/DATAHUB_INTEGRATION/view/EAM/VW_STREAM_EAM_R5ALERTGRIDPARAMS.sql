CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5ALERTGRIDPARAMS AS SELECT
                        src:AGP_ALERT::varchar AS AGP_ALERT, 
                        src:AGP_DVALUE::datetime AS AGP_DVALUE, 
                        src:AGP_LASTSAVED::datetime AS AGP_LASTSAVED, 
                        src:AGP_NVALUE::numeric(38, 10) AS AGP_NVALUE, 
                        src:AGP_PARAM::varchar AS AGP_PARAM, 
                        src:AGP_UPDATECOUNT::numeric(38, 10) AS AGP_UPDATECOUNT, 
                        src:AGP_VALUE::varchar AS AGP_VALUE, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:AGP_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:AGP_ALERT,
                src:AGP_PARAM  ORDER BY 
            src:AGP_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5ALERTGRIDPARAMS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AGP_DVALUE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AGP_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AGP_NVALUE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:AGP_UPDATECOUNT), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:AGP_LASTSAVED), '1900-01-01')) is not null