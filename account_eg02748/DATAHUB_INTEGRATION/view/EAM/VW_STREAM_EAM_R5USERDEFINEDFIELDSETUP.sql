CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5USERDEFINEDFIELDSETUP AS SELECT
                        src:UDF_CURR::varchar AS UDF_CURR, 
                        src:UDF_DATETYPE::varchar AS UDF_DATETYPE, 
                        src:UDF_ENABLEFORADDONS::varchar AS UDF_ENABLEFORADDONS, 
                        src:UDF_FIELD::varchar AS UDF_FIELD, 
                        src:UDF_LASTSAVED::datetime AS UDF_LASTSAVED, 
                        src:UDF_LOOKUPRENTITY::varchar AS UDF_LOOKUPRENTITY, 
                        src:UDF_LOOKUPTYPE::varchar AS UDF_LOOKUPTYPE, 
                        src:UDF_LOOKUPVALIDATE::varchar AS UDF_LOOKUPVALIDATE, 
                        src:UDF_MAX::varchar AS UDF_MAX, 
                        src:UDF_MIN::varchar AS UDF_MIN, 
                        src:UDF_NUMBERTYPE::varchar AS UDF_NUMBERTYPE, 
                        src:UDF_PRINT::varchar AS UDF_PRINT, 
                        src:UDF_RENTITY::varchar AS UDF_RENTITY, 
                        src:UDF_UOM::varchar AS UDF_UOM, 
                        src:UDF_UPDATECOUNT::numeric(38, 10) AS UDF_UPDATECOUNT, 
                        src:UDF_UPDATED::datetime AS UDF_UPDATED, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:UDF_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:UDF_FIELD,
                src:UDF_RENTITY  ORDER BY 
            src:UDF_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5USERDEFINEDFIELDSETUP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UDF_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:UDF_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UDF_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:UDF_LASTSAVED), '1900-01-01')) is not null