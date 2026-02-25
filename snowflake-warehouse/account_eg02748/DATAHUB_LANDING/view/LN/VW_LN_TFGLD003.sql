CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFGLD003 AS SELECT
                        src:bcmp::integer AS bcmp, 
                        src:cfst::integer AS cfst, 
                        src:cfst_kw::varchar AS cfst_kw, 
                        src:compnr::integer AS compnr, 
                        src:dcfi::integer AS dcfi, 
                        src:dcfi_kw::varchar AS dcfi_kw, 
                        src:deleted::boolean AS deleted, 
                        src:dhcu::object AS dhcu, 
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
                        src:dm10::integer AS dm10, 
                        src:dm10_kw::varchar AS dm10_kw, 
                        src:dm11::integer AS dm11, 
                        src:dm11_kw::varchar AS dm11_kw, 
                        src:dm12::integer AS dm12, 
                        src:dm12_kw::varchar AS dm12_kw, 
                        src:dsca::object AS dsca, 
                        src:gmbi::integer AS gmbi, 
                        src:gmbi_kw::varchar AS gmbi_kw, 
                        src:indt::datetime AS indt, 
                        src:nfpr::integer AS nfpr, 
                        src:nrpr::integer AS nrpr, 
                        src:nvpr::integer AS nvpr, 
                        src:papp::integer AS papp, 
                        src:papp_kw::varchar AS papp_kw, 
                        src:psbk::integer AS psbk, 
                        src:psbk_kw::varchar AS psbk_kw, 
                        src:psep::varchar AS psep, 
                        src:psic::integer AS psic, 
                        src:psic_kw::varchar AS psic_kw, 
                        src:pupt::integer AS pupt, 
                        src:pupt_kw::varchar AS pupt_kw, 
                        src:rper::integer AS rper, 
                        src:rper_kw::varchar AS rper_kw, 
                        src:sdat::integer AS sdat, 
                        src:sequencenumber::integer AS sequencenumber, 
                        src:sgmr::integer AS sgmr, 
                        src:sgmr_kw::varchar AS sgmr_kw, 
                        src:timestamp::datetime AS timestamp, 
                        src:username::varchar AS username, 
            CASE
                WHEN 'LN' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'LN' = 'IPS'
                THEN src:"DATALAKE_DELETED"::BOOLEAN
                ELSE src:"_DELETED"::BOOLEAN
            END as ETL_DELETED,
            etl_load_datetime,
            etl_load_metadatafilename,
            ETL_RANK,
            IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) as ETL_RANK_TIMESTAMP
            FROM 
            (
            select 
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROWNUMBER as ETL_RANK
                from
                (
                    SELECT
    
                        
                src:indt,
                src:compnr,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:indt,
                src:compnr  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFGLD003 as strm))