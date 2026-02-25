CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TRACKINGCTRL AS SELECT
                        src:TKC_COLUMN::varchar AS TKC_COLUMN, 
                        src:TKC_DATARTYPE::varchar AS TKC_DATARTYPE, 
                        src:TKC_DATATYPE::varchar AS TKC_DATATYPE, 
                        src:TKC_DISPLAYSEQ::numeric(38, 10) AS TKC_DISPLAYSEQ, 
                        src:TKC_INTERFACERTYPE::varchar AS TKC_INTERFACERTYPE, 
                        src:TKC_INTERFACETYPE::varchar AS TKC_INTERFACETYPE, 
                        src:TKC_LASTSAVED::datetime AS TKC_LASTSAVED, 
                        src:TKC_LENGTH::numeric(38, 10) AS TKC_LENGTH, 
                        src:TKC_RCOLUMN::varchar AS TKC_RCOLUMN, 
                        src:TKC_UPLOADCOLUMN::varchar AS TKC_UPLOADCOLUMN, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:TKC_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:TKC_INTERFACERTYPE,
                src:TKC_UPLOADCOLUMN  ORDER BY 
            src:TKC_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TRACKINGCTRL as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TKC_DISPLAYSEQ), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TKC_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TKC_LENGTH), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TKC_LASTSAVED), '1900-01-01')) is not null