CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_LN_TFFST121 AS SELECT
                        src:accn::varchar AS accn, 
                        src:cmpf::integer AS cmpf, 
                        src:cmpm::varchar AS cmpm, 
                        src:cmpt::integer AS cmpt, 
                        src:compnr::integer AS compnr, 
                        src:deleted::boolean AS deleted, 
                        src:dimf_1::varchar AS dimf_1, 
                        src:dimf_10::varchar AS dimf_10, 
                        src:dimf_11::varchar AS dimf_11, 
                        src:dimf_12::varchar AS dimf_12, 
                        src:dimf_2::varchar AS dimf_2, 
                        src:dimf_3::varchar AS dimf_3, 
                        src:dimf_4::varchar AS dimf_4, 
                        src:dimf_5::varchar AS dimf_5, 
                        src:dimf_6::varchar AS dimf_6, 
                        src:dimf_7::varchar AS dimf_7, 
                        src:dimf_8::varchar AS dimf_8, 
                        src:dimf_9::varchar AS dimf_9, 
                        src:dimm_1::varchar AS dimm_1, 
                        src:dimm_10::varchar AS dimm_10, 
                        src:dimm_11::varchar AS dimm_11, 
                        src:dimm_12::varchar AS dimm_12, 
                        src:dimm_2::varchar AS dimm_2, 
                        src:dimm_3::varchar AS dimm_3, 
                        src:dimm_4::varchar AS dimm_4, 
                        src:dimm_5::varchar AS dimm_5, 
                        src:dimm_6::varchar AS dimm_6, 
                        src:dimm_7::varchar AS dimm_7, 
                        src:dimm_8::varchar AS dimm_8, 
                        src:dimm_9::varchar AS dimm_9, 
                        src:dimt_1::varchar AS dimt_1, 
                        src:dimt_10::varchar AS dimt_10, 
                        src:dimt_11::varchar AS dimt_11, 
                        src:dimt_12::varchar AS dimt_12, 
                        src:dimt_2::varchar AS dimt_2, 
                        src:dimt_3::varchar AS dimt_3, 
                        src:dimt_4::varchar AS dimt_4, 
                        src:dimt_5::varchar AS dimt_5, 
                        src:dimt_6::varchar AS dimt_6, 
                        src:dimt_7::varchar AS dimt_7, 
                        src:dimt_8::varchar AS dimt_8, 
                        src:dimt_9::varchar AS dimt_9, 
                        src:fstm::varchar AS fstm, 
                        src:fstm_accn_ref_compnr::integer AS fstm_accn_ref_compnr, 
                        src:fstm_ref_compnr::integer AS fstm_ref_compnr, 
                        src:lacf::varchar AS lacf, 
                        src:lacm::varchar AS lacm, 
                        src:lact::varchar AS lact, 
                        src:seqn::integer AS seqn, 
                        src:sequencenumber::integer AS sequencenumber, 
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
    
                        
                src:fstm,
                src:seqn,
                src:compnr,
                src:accn,
            src:sequencenumber
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:fstm,
                src:seqn,
                src:compnr,
                src:accn  ORDER BY 
            src:sequencenumber desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.LN_TFFST121 as strm))