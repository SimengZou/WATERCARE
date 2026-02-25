CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_EAM_R5TRACKINGDATA AS SELECT
                        src:TKD_CHANGED::varchar AS TKD_CHANGED, 
                        src:TKD_CREATED::datetime AS TKD_CREATED, 
                        src:TKD_LASTSAVED::datetime AS TKD_LASTSAVED, 
                        src:TKD_PROMPTDATA1::varchar AS TKD_PROMPTDATA1, 
                        src:TKD_PROMPTDATA10::varchar AS TKD_PROMPTDATA10, 
                        src:TKD_PROMPTDATA11::varchar AS TKD_PROMPTDATA11, 
                        src:TKD_PROMPTDATA12::varchar AS TKD_PROMPTDATA12, 
                        src:TKD_PROMPTDATA13::varchar AS TKD_PROMPTDATA13, 
                        src:TKD_PROMPTDATA14::varchar AS TKD_PROMPTDATA14, 
                        src:TKD_PROMPTDATA15::varchar AS TKD_PROMPTDATA15, 
                        src:TKD_PROMPTDATA16::varchar AS TKD_PROMPTDATA16, 
                        src:TKD_PROMPTDATA17::varchar AS TKD_PROMPTDATA17, 
                        src:TKD_PROMPTDATA18::varchar AS TKD_PROMPTDATA18, 
                        src:TKD_PROMPTDATA19::varchar AS TKD_PROMPTDATA19, 
                        src:TKD_PROMPTDATA2::varchar AS TKD_PROMPTDATA2, 
                        src:TKD_PROMPTDATA20::varchar AS TKD_PROMPTDATA20, 
                        src:TKD_PROMPTDATA21::varchar AS TKD_PROMPTDATA21, 
                        src:TKD_PROMPTDATA22::varchar AS TKD_PROMPTDATA22, 
                        src:TKD_PROMPTDATA23::varchar AS TKD_PROMPTDATA23, 
                        src:TKD_PROMPTDATA24::varchar AS TKD_PROMPTDATA24, 
                        src:TKD_PROMPTDATA25::varchar AS TKD_PROMPTDATA25, 
                        src:TKD_PROMPTDATA26::varchar AS TKD_PROMPTDATA26, 
                        src:TKD_PROMPTDATA27::varchar AS TKD_PROMPTDATA27, 
                        src:TKD_PROMPTDATA28::varchar AS TKD_PROMPTDATA28, 
                        src:TKD_PROMPTDATA29::varchar AS TKD_PROMPTDATA29, 
                        src:TKD_PROMPTDATA3::varchar AS TKD_PROMPTDATA3, 
                        src:TKD_PROMPTDATA30::varchar AS TKD_PROMPTDATA30, 
                        src:TKD_PROMPTDATA31::varchar AS TKD_PROMPTDATA31, 
                        src:TKD_PROMPTDATA32::varchar AS TKD_PROMPTDATA32, 
                        src:TKD_PROMPTDATA33::varchar AS TKD_PROMPTDATA33, 
                        src:TKD_PROMPTDATA34::varchar AS TKD_PROMPTDATA34, 
                        src:TKD_PROMPTDATA35::varchar AS TKD_PROMPTDATA35, 
                        src:TKD_PROMPTDATA36::varchar AS TKD_PROMPTDATA36, 
                        src:TKD_PROMPTDATA37::varchar AS TKD_PROMPTDATA37, 
                        src:TKD_PROMPTDATA38::varchar AS TKD_PROMPTDATA38, 
                        src:TKD_PROMPTDATA39::varchar AS TKD_PROMPTDATA39, 
                        src:TKD_PROMPTDATA4::varchar AS TKD_PROMPTDATA4, 
                        src:TKD_PROMPTDATA40::varchar AS TKD_PROMPTDATA40, 
                        src:TKD_PROMPTDATA41::varchar AS TKD_PROMPTDATA41, 
                        src:TKD_PROMPTDATA42::varchar AS TKD_PROMPTDATA42, 
                        src:TKD_PROMPTDATA43::varchar AS TKD_PROMPTDATA43, 
                        src:TKD_PROMPTDATA44::varchar AS TKD_PROMPTDATA44, 
                        src:TKD_PROMPTDATA45::varchar AS TKD_PROMPTDATA45, 
                        src:TKD_PROMPTDATA46::varchar AS TKD_PROMPTDATA46, 
                        src:TKD_PROMPTDATA47::varchar AS TKD_PROMPTDATA47, 
                        src:TKD_PROMPTDATA48::varchar AS TKD_PROMPTDATA48, 
                        src:TKD_PROMPTDATA49::varchar AS TKD_PROMPTDATA49, 
                        src:TKD_PROMPTDATA5::varchar AS TKD_PROMPTDATA5, 
                        src:TKD_PROMPTDATA50::varchar AS TKD_PROMPTDATA50, 
                        src:TKD_PROMPTDATA51::varchar AS TKD_PROMPTDATA51, 
                        src:TKD_PROMPTDATA6::varchar AS TKD_PROMPTDATA6, 
                        src:TKD_PROMPTDATA7::varchar AS TKD_PROMPTDATA7, 
                        src:TKD_PROMPTDATA8::varchar AS TKD_PROMPTDATA8, 
                        src:TKD_PROMPTDATA9::varchar AS TKD_PROMPTDATA9, 
                        src:TKD_RSTATUS::varchar AS TKD_RSTATUS, 
                        src:TKD_SESSIONID::numeric(38, 10) AS TKD_SESSIONID, 
                        src:TKD_SOURCECODE::varchar AS TKD_SOURCECODE, 
                        src:TKD_SOURCESYSTEM::varchar AS TKD_SOURCESYSTEM, 
                        src:TKD_TRACKDATE::datetime AS TKD_TRACKDATE, 
                        src:TKD_TRANS::varchar AS TKD_TRANS, 
                        src:TKD_TRANSID::numeric(38, 10) AS TKD_TRANSID, 
                        src:_DELETED::boolean AS _DELETED, 
                        src:TKD_LASTSAVED::datetime as ETL_LASTSAVED,
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
                                        
                src:TKD_TRANSID  ORDER BY 
            src:TKD_LASTSAVED desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_EAM_R5TRACKINGDATA as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TKD_CREATED), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TKD_LASTSAVED), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TKD_SESSIONID), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TKD_TRACKDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:TKD_TRANSID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:TKD_LASTSAVED), '1900-01-01')) is not null