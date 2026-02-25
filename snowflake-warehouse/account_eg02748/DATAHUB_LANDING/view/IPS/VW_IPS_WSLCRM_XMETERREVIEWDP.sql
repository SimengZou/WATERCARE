CREATE OR REPLACE VIEW DATAHUB_LANDING.VW_IPS_WSLCRM_XMETERREVIEWDP AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPROVALDATE::datetime AS APPROVALDATE, 
                        src:CHECKREADISCHARGEABLE::varchar AS CHECKREADISCHARGEABLE, 
                        src:COMMENTS::varchar AS COMMENTS, 
                        src:DELETED::boolean AS DELETED, 
                        src:ISCHECKREADREQUEST::varchar AS ISCHECKREADREQUEST, 
                        src:ISREADAPPROVED::varchar AS ISREADAPPROVED, 
                        src:ISREADNOACTION::varchar AS ISREADNOACTION, 
                        src:ISREADREJECTED::varchar AS ISREADREJECTED, 
                        src:MARKASANOMALY::varchar AS MARKASANOMALY, 
                        src:MARKASNOREAD::varchar AS MARKASNOREAD, 
                        src:MARKASREADYTOBILL::varchar AS MARKASREADYTOBILL, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:PHOTOREQUIRED::varchar AS PHOTOREQUIRED, 
                        src:REASSIGNTO::varchar AS REASSIGNTO, 
                        src:REJECTEDDATE::datetime AS REJECTEDDATE, 
                        src:SERVNO::integer AS SERVNO, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:XMETERREVIEWDPKEY::integer AS XMETERREVIEWDPKEY, 
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
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
    
                        
                src:XMETERREVIEWDPKEY,
            src:VARIATION_ID
                ,src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                src:XMETERREVIEWDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_LANDING.IPS_WSLCRM_XMETERREVIEWDP as strm))