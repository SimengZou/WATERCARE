CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFFBS003 AS SELECT
                        src:ad10::integer AS ad10, 
                        src:ad10_kw::varchar AS ad10_kw, 
                        src:ad11::integer AS ad11, 
                        src:ad11_kw::varchar AS ad11_kw, 
                        src:ad12::integer AS ad12, 
                        src:ad12_kw::varchar AS ad12_kw, 
                        src:adt1::integer AS adt1, 
                        src:adt1_kw::varchar AS adt1_kw, 
                        src:adt2::integer AS adt2, 
                        src:adt2_kw::varchar AS adt2_kw, 
                        src:adt3::integer AS adt3, 
                        src:adt3_kw::varchar AS adt3_kw, 
                        src:adt4::integer AS adt4, 
                        src:adt4_kw::varchar AS adt4_kw, 
                        src:adt5::integer AS adt5, 
                        src:adt5_kw::varchar AS adt5_kw, 
                        src:adt6::integer AS adt6, 
                        src:adt6_kw::varchar AS adt6_kw, 
                        src:adt7::integer AS adt7, 
                        src:adt7_kw::varchar AS adt7_kw, 
                        src:adt8::integer AS adt8, 
                        src:adt8_kw::varchar AS adt8_kw, 
                        src:adt9::integer AS adt9, 
                        src:adt9_kw::varchar AS adt9_kw, 
                        src:aqu1::integer AS aqu1, 
                        src:aqu1_kw::varchar AS aqu1_kw, 
                        src:aqu2::integer AS aqu2, 
                        src:aqu2_kw::varchar AS aqu2_kw, 
                        src:budg::varchar AS budg, 
                        src:budm::integer AS budm, 
                        src:budm_kw::varchar AS budm_kw, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:llac::integer AS llac, 
                        src:llac_kw::varchar AS llac_kw, 
                        src:nbpr::integer AS nbpr, 
                        src:sdbu::integer AS sdbu, 
                        src:sdbu_kw::varchar AS sdbu_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:text::integer AS text, 
                        src:text_ref_compnr::integer AS text_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'LN' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'LN' = 'IPS'
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
                                        
                src:budg,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFFBS003 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ad10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ad11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ad12), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adt1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adt2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adt3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adt4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adt5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adt6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adt7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adt8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:adt9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aqu1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aqu2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:budm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:llac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nbpr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sdbu), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null