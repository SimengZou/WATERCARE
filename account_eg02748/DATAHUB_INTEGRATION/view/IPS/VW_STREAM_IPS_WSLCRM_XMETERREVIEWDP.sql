CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCRM_XMETERREVIEWDP AS SELECT
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
                        src:XMETERREVIEWDPKEY::integer AS XMETERREVIEWDPKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
            CASE
                WHEN 'IPS' = 'LN'
                THEN src:"deleted"::BOOLEAN
                WHEN 'IPS' = 'IPS'
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
                                        
                src:XMETERREVIEWDPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCRM_XMETERREVIEWDP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:APPROVALDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:REJECTEDDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XMETERREVIEWDPKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null