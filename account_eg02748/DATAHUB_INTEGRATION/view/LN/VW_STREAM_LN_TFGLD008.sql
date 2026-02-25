CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_LN_TFGLD008 AS SELECT
                        src:acmp::varchar AS acmp, 
                        src:acmp_ref_compnr::integer AS acmp_ref_compnr, 
                        src:alat::integer AS alat, 
                        src:alat_kw::varchar AS alat_kw, 
                        src:atyp::integer AS atyp, 
                        src:atyp_kw::varchar AS atyp_kw, 
                        src:bfdt::date AS bfdt, 
                        src:blbp::integer AS blbp, 
                        src:blbp_kw::varchar AS blbp_kw, 
                        src:bloc::integer AS bloc, 
                        src:bloc_kw::varchar AS bloc_kw, 
                        src:budt::date AS budt, 
                        src:cdca::varchar AS cdca, 
                        src:cdca_ref_compnr::integer AS cdca_ref_compnr, 
                        src:cfic::varchar AS cfic, 
                        src:cfic_ref_compnr::integer AS cfic_ref_compnr, 
                        src:cfrs::varchar AS cfrs, 
                        src:cfrs_ref_compnr::integer AS cfrs_ref_compnr, 
                        src:compnr::integer AS compnr, 
                        src:ctar::integer AS ctar, 
                        src:ctar_kw::varchar AS ctar_kw, 
                        src:ctlm::integer AS ctlm, 
                        src:ctlm_kw::varchar AS ctlm_kw, 
                        src:cvat::varchar AS cvat, 
                        src:cvat_ref_compnr::integer AS cvat_ref_compnr, 
                        src:dbcr::integer AS dbcr, 
                        src:dbcr_kw::varchar AS dbcr_kw, 
                        src:dblm::integer AS dblm, 
                        src:dblm_kw::varchar AS dblm_kw, 
                        src:deleted::boolean AS deleted, 
                        src:desc::object AS desc, 
                        src:dga1::varchar AS dga1, 
                        src:dga1_ref_compnr::integer AS dga1_ref_compnr, 
                        src:dga2::varchar AS dga2, 
                        src:dga2_ref_compnr::integer AS dga2_ref_compnr, 
                        src:dim1::integer AS dim1, 
                        src:dim1_kw::varchar AS dim1_kw, 
                        src:dim2::integer AS dim2, 
                        src:dim2_kw::varchar AS dim2_kw, 
                        src:dim3::integer AS dim3, 
                        src:dim3_kw::varchar AS dim3_kw, 
                        src:dim4::integer AS dim4, 
                        src:dim4_kw::varchar AS dim4_kw, 
                        src:dim5::integer AS dim5, 
                        src:dim5_kw::varchar AS dim5_kw, 
                        src:dim6::integer AS dim6, 
                        src:dim6_kw::varchar AS dim6_kw, 
                        src:dim7::integer AS dim7, 
                        src:dim7_kw::varchar AS dim7_kw, 
                        src:dim8::integer AS dim8, 
                        src:dim8_kw::varchar AS dim8_kw, 
                        src:dim9::integer AS dim9, 
                        src:dim9_kw::varchar AS dim9_kw, 
                        src:dla1::varchar AS dla1, 
                        src:dla1_ref_compnr::integer AS dla1_ref_compnr, 
                        src:dla2::varchar AS dla2, 
                        src:dla2_ref_compnr::integer AS dla2_ref_compnr, 
                        src:dm10::integer AS dm10, 
                        src:dm10_kw::varchar AS dm10_kw, 
                        src:dm11::integer AS dm11, 
                        src:dm11_kw::varchar AS dm11_kw, 
                        src:dm12::integer AS dm12, 
                        src:dm12_kw::varchar AS dm12_kw, 
                        src:dmsq::integer AS dmsq, 
                        src:duac::integer AS duac, 
                        src:duac_kw::varchar AS duac_kw, 
                        src:etyp::integer AS etyp, 
                        src:etyp_kw::varchar AS etyp_kw, 
                        src:icom::integer AS icom, 
                        src:icom_kw::varchar AS icom_kw, 
                        src:icur::integer AS icur, 
                        src:icur_kw::varchar AS icur_kw, 
                        src:ifas::integer AS ifas, 
                        src:ifas_kw::varchar AS ifas_kw, 
                        src:injb::integer AS injb, 
                        src:injb_kw::varchar AS injb_kw, 
                        src:inta::integer AS inta, 
                        src:inta_kw::varchar AS inta_kw, 
                        src:iprj::integer AS iprj, 
                        src:iprj_kw::varchar AS iprj_kw, 
                        src:leac::varchar AS leac, 
                        src:ledc::object AS ledc, 
                        src:lela::varchar AS lela, 
                        src:loco::integer AS loco, 
                        src:mach::integer AS mach, 
                        src:mach_kw::varchar AS mach_kw, 
                        src:pcac::varchar AS pcac, 
                        src:pcac_ref_compnr::integer AS pcac_ref_compnr, 
                        src:perc::numeric(38, 17) AS perc, 
                        src:plac::varchar AS plac, 
                        src:plac_ref_compnr::integer AS plac_ref_compnr, 
                        src:pseq::integer AS pseq, 
                        src:sear::varchar AS sear, 
                        src:sear_ref_compnr::integer AS sear_ref_compnr, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:skey::object AS skey, 
                        src:subl::integer AS subl, 
                        src:tagp::varchar AS tagp, 
                        src:tagp_ref_compnr::integer AS tagp_ref_compnr, 
                        src:text::integer AS text, 
                        src:text_ref_compnr::integer AS text_ref_compnr, 
                        src:timestamp::datetime AS timestamp, 
                        src:uni1::varchar AS uni1, 
                        src:uni1_ref_compnr::integer AS uni1_ref_compnr, 
                        src:uni2::varchar AS uni2, 
                        src:uni2_ref_compnr::integer AS uni2_ref_compnr, 
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
                src:leac  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_LN_TFGLD008 as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:acmp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:alat), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:atyp), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:bfdt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:blbp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:bloc), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:budt), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cdca_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfic_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cfrs_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctar), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ctlm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:cvat_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dbcr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dblm), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dga1_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dga2_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dim1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dim2), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dim3), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dim4), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dim5), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dim6), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dim7), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dim8), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dim9), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dla1_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dla2_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dm10), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dm11), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dm12), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:dmsq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:duac), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:etyp), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:icom), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:icur), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ifas), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:injb), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:inta), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:iprj), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:loco), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:mach), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pcac_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:perc), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:plac_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:pseq), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sear_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:sequencenumber), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:subl), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:tagp_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:text_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:timestamp), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uni1_ref_compnr), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:uni2_ref_compnr), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:sequencenumber), '1900-01-01')) is not null