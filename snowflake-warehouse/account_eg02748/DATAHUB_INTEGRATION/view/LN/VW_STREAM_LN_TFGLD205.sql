CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD205 AS SELECT
                        src:bpid::varchar AS bpid, 
                        src:ccur::varchar AS ccur, 
                        src:ccur_ref_compnr::integer AS ccur_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:cono::integer AS cono, 
                        src:deleted::boolean AS deleted, 
                        src:dim1::varchar AS dim1, 
                        src:dim2::varchar AS dim2, 
                        src:dim3::varchar AS dim3, 
                        src:dim4::varchar AS dim4, 
                        src:dim5::varchar AS dim5, 
                        src:dim6::varchar AS dim6, 
                        src:dim7::varchar AS dim7, 
                        src:dim8::varchar AS dim8, 
                        src:dim9::varchar AS dim9, 
                        src:dims::varchar AS dims, 
                        src:dimx_sgm1::varchar AS dimx_sgm1, 
                        src:dimx_sgm2::varchar AS dimx_sgm2, 
                        src:dm10::varchar AS dm10, 
                        src:dm11::varchar AS dm11, 
                        src:dm12::varchar AS dm12, 
                        src:duac::integer AS duac, 
                        src:duac_kw::varchar AS duac_kw, 
                        src:fcah_1::numeric(38, 17) AS fcah_1, 
                        src:fcah_2::numeric(38, 17) AS fcah_2, 
                        src:fcah_3::numeric(38, 17) AS fcah_3, 
                        src:fcah_dtwc::numeric(38, 17) AS fcah_dtwc, 
                        src:fcah_rfrc::numeric(38, 17) AS fcah_rfrc, 
                        src:fcam::numeric(38, 17) AS fcam, 
                        src:fdah_1::numeric(38, 17) AS fdah_1, 
                        src:fdah_2::numeric(38, 17) AS fdah_2, 
                        src:fdah_3::numeric(38, 17) AS fdah_3, 
                        src:fdah_dtwc::numeric(38, 17) AS fdah_dtwc, 
                        src:fdah_rfrc::numeric(38, 17) AS fdah_rfrc, 
                        src:fdam::numeric(38, 17) AS fdam, 
                        src:fqt1::numeric(38, 17) AS fqt1, 
                        src:fqt2::numeric(38, 17) AS fqt2, 
                        src:leac::varchar AS leac, 
                        src:ncah_1::numeric(38, 17) AS ncah_1, 
                        src:ncah_2::numeric(38, 17) AS ncah_2, 
                        src:ncah_3::numeric(38, 17) AS ncah_3, 
                        src:ncah_dtwc::numeric(38, 17) AS ncah_dtwc, 
                        src:ncah_rfrc::numeric(38, 17) AS ncah_rfrc, 
                        src:ncam::numeric(38, 17) AS ncam, 
                        src:ndah_1::numeric(38, 17) AS ndah_1, 
                        src:ndah_2::numeric(38, 17) AS ndah_2, 
                        src:ndah_3::numeric(38, 17) AS ndah_3, 
                        src:ndah_dtwc::numeric(38, 17) AS ndah_dtwc, 
                        src:ndah_rfrc::numeric(38, 17) AS ndah_rfrc, 
                        src:ndam::numeric(38, 17) AS ndam, 
                        src:nqt1::numeric(38, 17) AS nqt1, 
                        src:nqt2::numeric(38, 17) AS nqt2, 
                        src:olap::integer AS olap, 
                        src:prno::integer AS prno, 
                        src:ptyp::integer AS ptyp, 
                        src:ptyp_kw::varchar AS ptyp_kw, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, 
                        src:year::integer AS year, src:sequencenumber::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:cono,
                src:prno,
                src:leac,
                src:ccur,
                src:bpid,
                src:dim4,
                src:dim3,
                src:dims,
                src:dim1,
                src:duac,
                src:compnr,
                src:dim2,
                src:dim5,
                src:year,
                src:ptyp  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD205 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ccur_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cono), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:duac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcah_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcah_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcah_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcah_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcah_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdah_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdah_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdah_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdah_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdah_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fdam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fqt1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fqt2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ncah_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ncah_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ncah_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ncah_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ncah_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ncam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ndah_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ndah_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ndah_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ndah_dtwc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ndah_rfrc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ndam), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nqt1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nqt2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:olap), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:prno), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ptyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null