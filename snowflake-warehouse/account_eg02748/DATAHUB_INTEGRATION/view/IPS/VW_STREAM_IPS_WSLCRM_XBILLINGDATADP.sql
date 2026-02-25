CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCRM_XBILLINGDATADP AS SELECT
                        src:ACCOUNTCLASS::varchar AS ACCOUNTCLASS, 
                        src:ACCOUNTCREATEDATE::datetime AS ACCOUNTCREATEDATE, 
                        src:ACTIONTYPEDESC::varchar AS ACTIONTYPEDESC, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESS::varchar AS ADDRESS, 
                        src:APPNO::varchar AS APPNO, 
                        src:CONTACTID::varchar AS CONTACTID, 
                        src:CONTACTKEY::integer AS CONTACTKEY, 
                        src:CONTRACTORREFNO::varchar AS CONTRACTORREFNO, 
                        src:CREATEACCOUNT::varchar AS CREATEACCOUNT, 
                        src:CREWID::varchar AS CREWID, 
                        src:CT::varchar AS CT, 
                        src:CUSTOMERSIDE::varchar AS CUSTOMERSIDE, 
                        src:DELETED::boolean AS DELETED, 
                        src:DISCONNECTIONDATE::datetime AS DISCONNECTIONDATE, 
                        src:DISCONNECTIONTYPE::varchar AS DISCONNECTIONTYPE, 
                        src:DP::varchar AS DP, 
                        src:EMPLOYEEID::varchar AS EMPLOYEEID, 
                        src:ESTDAILYWATERDEMAND::numeric(38, 10) AS ESTDAILYWATERDEMAND, 
                        src:FLOWATTHEMETER::numeric(38, 10) AS FLOWATTHEMETER, 
                        src:GISXCOORD::numeric(38, 10) AS GISXCOORD, 
                        src:GISYCOORD::numeric(38, 10) AS GISYCOORD, 
                        src:GISZCOORD::numeric(38, 10) AS GISZCOORD, 
                        src:INITIALMETERREADING::numeric(38, 10) AS INITIALMETERREADING, 
                        src:LOCATIONDESCRIPTION::varchar AS LOCATIONDESCRIPTION, 
                        src:LOT::varchar AS LOT, 
                        src:METERINSTALLATIONPHOTO::varchar AS METERINSTALLATIONPHOTO, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:MULTIMETERACCOUNT::varchar AS MULTIMETERACCOUNT, 
                        src:NEWMETERID::varchar AS NEWMETERID, 
                        src:NEWMETERINSTALLDATE::datetime AS NEWMETERINSTALLDATE, 
                        src:NEWMETERSIZE::varchar AS NEWMETERSIZE, 
                        src:OLDMETERID::varchar AS OLDMETERID, 
                        src:OLDMETERREADING::numeric(38, 10) AS OLDMETERREADING, 
                        src:OWNERCUSTOMER::varchar AS OWNERCUSTOMER, 
                        src:PHONENUMBER::varchar AS PHONENUMBER, 
                        src:PIPEDAIMETER::integer AS PIPEDAIMETER, 
                        src:PIPELENGTH::numeric(38, 10) AS PIPELENGTH, 
                        src:PRICEPLAN::varchar AS PRICEPLAN, 
                        src:REJECTBYBILLS::varchar AS REJECTBYBILLS, 
                        src:REJECTREASON::varchar AS REJECTREASON, 
                        src:SERIALNO::varchar AS SERIALNO, 
                        src:SERVICELINEMATERIAL::varchar AS SERVICELINEMATERIAL, 
                        src:SERVNO::integer AS SERVNO, 
                        src:SMS::varchar AS SMS, 
                        src:SUBMITTEDASBUILT::varchar AS SUBMITTEDASBUILT, 
                        src:USELASTBILLEDREAD::varchar AS USELASTBILLEDREAD, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WORKORDERMETERID::varchar AS WORKORDERMETERID, 
                        src:WORKORDERNO::varchar AS WORKORDERNO, 
                        src:WORKTYPEDESC::varchar AS WORKTYPEDESC, 
                        src:WSLSIDE::varchar AS WSLSIDE, 
                        src:XBILLINGDATADPKEY::integer AS XBILLINGDATADPKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XBILLINGDATADPKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCRM_XBILLINGDATADP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ACCOUNTCREATEDATE), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CONTACTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:DISCONNECTIONDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESTDAILYWATERDEMAND), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:FLOWATTHEMETER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GISXCOORD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GISYCOORD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:GISZCOORD), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INITIALMETERREADING), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:NEWMETERINSTALLDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OLDMETERREADING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PIPEDAIMETER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PIPELENGTH), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SERVNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XBILLINGDATADPKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null