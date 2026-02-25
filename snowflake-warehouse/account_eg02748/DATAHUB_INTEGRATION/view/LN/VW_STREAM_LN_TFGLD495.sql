CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD495 AS SELECT
                        src:acce::integer AS acce, 
                        src:acce_kw::varchar AS acce_kw, 
                        src:accf::integer AS accf, 
                        src:accf_kw::varchar AS accf_kw, 
                        src:amnt::numeric(38, 17) AS amnt, 
                        src:amth_1::numeric(38, 17) AS amth_1, 
                        src:amth_2::numeric(38, 17) AS amth_2, 
                        src:amth_3::numeric(38, 17) AS amth_3, 
                        src:aobl::integer AS aobl, 
                        src:aobl_kw::varchar AS aobl_kw, 
                        src:arat_1::numeric(38, 17) AS arat_1, 
                        src:arat_2::numeric(38, 17) AS arat_2, 
                        src:arat_3::numeric(38, 17) AS arat_3, 
                        src:blrf::varchar AS blrf, 
                        src:btno::integer AS btno, 
                        src:buid::varchar AS buid, 
                        src:ccur::varchar AS ccur, 
                        src:compnr::integer AS compnr, 
                        src:crdt::datetime AS crdt, 
                        src:cuni::varchar AS cuni, 
                        src:dbcr::integer AS dbcr, 
                        src:dbcr_kw::varchar AS dbcr_kw, 
                        src:dcdt::date AS dcdt, 
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
                        src:dm10::varchar AS dm10, 
                        src:dm11::varchar AS dm11, 
                        src:dm12::varchar AS dm12, 
                        src:docn::integer AS docn, 
                        src:fcom::integer AS fcom, 
                        src:fprd::integer AS fprd, 
                        src:fyer::integer AS fyer, 
                        src:guid::varchar AS guid, 
                        src:idtc::varchar AS idtc, 
                        src:inad::integer AS inad, 
                        src:inad_kw::varchar AS inad_kw, 
                        src:inic::integer AS inic, 
                        src:inic_kw::varchar AS inic_kw, 
                        src:ktrn::integer AS ktrn, 
                        src:ktrn_kw::varchar AS ktrn_kw, 
                        src:lcit::integer AS lcit, 
                        src:leac::varchar AS leac, 
                        src:link::varchar AS link, 
                        src:lino::integer AS lino, 
                        src:nuni::numeric(38, 17) AS nuni, 
                        src:obre::varchar AS obre, 
                        src:ocmp::integer AS ocmp, 
                        src:odbc::integer AS odbc, 
                        src:odbc_kw::varchar AS odbc_kw, 
                        src:ogui::varchar AS ogui, 
                        src:podt::datetime AS podt, 
                        src:proj::varchar AS proj, 
                        src:rbid::varchar AS rbid, 
                        src:rbon::varchar AS rbon, 
                        src:rce1::varchar AS rce1, 
                        src:rce2::varchar AS rce2, 
                        src:rce3::varchar AS rce3, 
                        src:rce4::varchar AS rce4, 
                        src:rce5::varchar AS rce5, 
                        src:reco::integer AS reco, 
                        src:reco_kw::varchar AS reco_kw, 
                        src:recs::integer AS recs, 
                        src:rev1::varchar AS rev1, 
                        src:rev2::varchar AS rev2, 
                        src:rev3::varchar AS rev3, 
                        src:rev4::varchar AS rev4, 
                        src:rev5::varchar AS rev5, 
                        src:rpon::integer AS rpon, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:serl::integer AS serl, 
                        src:srno::integer AS srno, 
                        src:tdte::date AS tdte, 
                        src:timestamp::datetime AS timestamp, 
                        src:trdt::datetime AS trdt, 
                        src:ttyp::varchar AS ttyp, 
                        src:urat_1::numeric(38, 17) AS urat_1, 
                        src:urat_2::numeric(38, 17) AS urat_2, 
                        src:urat_3::numeric(38, 17) AS urat_3, 
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
                                        
                src:guid,
                src:dbcr,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD495 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acce), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:accf), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amnt), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:amth_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:aobl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:arat_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:arat_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:arat_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:btno), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:crdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dbcr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:dcdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:docn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fcom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fprd), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:fyer), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inad), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inic), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ktrn), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lcit), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:lino), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:nuni), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ocmp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:odbc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:podt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:reco), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:recs), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:rpon), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:serl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:srno), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:tdte), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:trdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:urat_1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:urat_2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:urat_3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:year), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null