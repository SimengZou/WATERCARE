CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFACP270 AS SELECT
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:amth_1::numeric(38, 17) AS amth_1, 
                        src:amth_2::numeric(38, 17) AS amth_2, 
                        src:amth_3::numeric(38, 17) AS amth_3, 
                        src:buid::varchar AS buid, 
                        src:cact::varchar AS cact, 
                        src:ccur::varchar AS ccur, 
                        src:clss::integer AS clss, 
                        src:compnr::integer AS compnr, 
                        src:cprj::varchar AS cprj, 
                        src:cspa::varchar AS cspa, 
                        src:cuni::varchar AS cuni, 
                        src:dbcr::integer AS dbcr, 
                        src:dbcr_kw::varchar AS dbcr_kw, 
                        src:dcom::integer AS dcom, 
                        src:deleted::boolean AS deleted, 
                        src:docn::integer AS docn, 
                        src:fcom::integer AS fcom, 
                        src:guid::varchar AS guid, 
                        src:idtc::varchar AS idtc, 
                        src:ktrn::integer AS ktrn, 
                        src:ktrn_kw::varchar AS ktrn_kw, 
                        src:nuni::numeric(38, 17) AS nuni, 
                        src:obre::varchar AS obre, 
                        src:ocmp::integer AS ocmp, 
                        src:rbid::varchar AS rbid, 
                        src:rbon::varchar AS rbon, 
                        src:reco::integer AS reco, 
                        src:reco_kw::varchar AS reco_kw, 
                        src:recs::integer AS recs, 
                        src:rpon::integer AS rpon, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:timestamp::datetime AS timestamp, 
                        src:ttyp::varchar AS ttyp, 
                        src:txin::integer AS txin, 
                        src:txin_kw::varchar AS txin_kw, 
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
                                        
                src:compnr,
                src:guid,
                src:dbcr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFACP270 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:clss), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dbcr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dcom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:docn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ktrn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nuni), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ocmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:reco), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:recs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rpon), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:txin), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null