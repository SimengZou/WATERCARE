CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRPROJ_NETWORKSHUTDOWNREQUEST AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:APPLICANTISPOC::varchar AS APPLICANTISPOC, 
                        src:APPROJAPPLDTLKEY::integer AS APPROJAPPLDTLKEY, 
                        src:BILLCITY::varchar AS BILLCITY, 
                        src:BILLCOMPANY::varchar AS BILLCOMPANY, 
                        src:BILLEMAIL::varchar AS BILLEMAIL, 
                        src:BILLFIRSTNAME::varchar AS BILLFIRSTNAME, 
                        src:BILLLASTNAME::varchar AS BILLLASTNAME, 
                        src:BILLMOBILE::varchar AS BILLMOBILE, 
                        src:BILLPHONE::varchar AS BILLPHONE, 
                        src:BILLPOSTCODE::varchar AS BILLPOSTCODE, 
                        src:BILLSTREETNAME::varchar AS BILLSTREETNAME, 
                        src:BILLSTREETNUMBER::varchar AS BILLSTREETNUMBER, 
                        src:BILLSUBURB::varchar AS BILLSUBURB, 
                        src:BILLTITLE::varchar AS BILLTITLE, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DECLARATION::varchar AS DECLARATION, 
                        src:DETAILCONNMETHODATTACH::varchar AS DETAILCONNMETHODATTACH, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NETWORKSHUTDOWNREQUESTKEY::integer AS NETWORKSHUTDOWNREQUESTKEY, 
                        src:PAYEETYPE::varchar AS PAYEETYPE, 
                        src:POCCITY::varchar AS POCCITY, 
                        src:POCCOMPANY::varchar AS POCCOMPANY, 
                        src:POCEMAIL::varchar AS POCEMAIL, 
                        src:POCFIRSTNMAE::varchar AS POCFIRSTNMAE, 
                        src:POCLASTNAME::varchar AS POCLASTNAME, 
                        src:POCMOBILE::varchar AS POCMOBILE, 
                        src:POCPOSTCODE::varchar AS POCPOSTCODE, 
                        src:POCSTREETNAME::varchar AS POCSTREETNAME, 
                        src:POCSTREETNUMBER::varchar AS POCSTREETNUMBER, 
                        src:POCSUBURB::varchar AS POCSUBURB, 
                        src:POCTITLE::varchar AS POCTITLE, 
                        src:POCWORKPHONE::varchar AS POCWORKPHONE, 
                        src:PROPOSEDCONNECTIONDATE::datetime AS PROPOSEDCONNECTIONDATE, 
                        src:REQUESTDATE::datetime AS REQUESTDATE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:NETWORKSHUTDOWNREQUESTKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRPROJ_NETWORKSHUTDOWNREQUEST as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APPROJAPPLDTLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NETWORKSHUTDOWNREQUESTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:PROPOSEDCONNECTIONDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:REQUESTDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null