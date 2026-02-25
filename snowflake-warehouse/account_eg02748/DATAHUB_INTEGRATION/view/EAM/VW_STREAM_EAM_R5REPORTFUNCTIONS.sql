CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5REPORTFUNCTIONS AS SELECT
                        src:RFN_FIELDORDER::varchar AS RFN_FIELDORDER, 
                        src:RFN_FROMCLAUSE::varchar AS RFN_FROMCLAUSE, 
                        src:RFN_FUNCTION::varchar AS RFN_FUNCTION, 
                        src:RFN_GROUPBY::varchar AS RFN_GROUPBY, 
                        src:RFN_LASTSAVED::datetime AS RFN_LASTSAVED, 
                        src:RFN_ORDERBY::varchar AS RFN_ORDERBY, 
                        src:RFN_ORDERTYPE::varchar AS RFN_ORDERTYPE, 
                        src:RFN_UPDATECOUNT::numeric(38, 10) AS RFN_UPDATECOUNT, 
                        src:RFN_UPDATED::datetime AS RFN_UPDATED, 
                        src:RFN_WHERECLAUSE1::varchar AS RFN_WHERECLAUSE1, 
                        src:RFN_WHERECLAUSE2::varchar AS RFN_WHERECLAUSE2, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:RFN_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:RFN_FUNCTION  ORDER BY 
            src:RFN_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5REPORTFUNCTIONS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RFN_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:RFN_UPDATECOUNT), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RFN_UPDATED), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RFN_LASTSAVED), '1900-01-01')) is not null