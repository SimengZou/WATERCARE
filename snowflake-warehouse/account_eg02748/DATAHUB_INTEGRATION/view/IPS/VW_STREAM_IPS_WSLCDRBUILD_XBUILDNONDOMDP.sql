CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_WSLCDRBUILD_XBUILDNONDOMDP AS SELECT
                        src:ACCOUNTNO::varchar AS ACCOUNTNO, 
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDITIONALDETAILS::varchar AS ADDITIONALDETAILS, 
                        src:APBLDGAPPLDTLKEY::integer AS APBLDGAPPLDTLKEY, 
                        src:BACKFLOWTYPE::varchar AS BACKFLOWTYPE, 
                        src:BILLADDRESS::varchar AS BILLADDRESS, 
                        src:BILLCLASS::varchar AS BILLCLASS, 
                        src:BILLMETHOD::varchar AS BILLMETHOD, 
                        src:BILLOWNERORAPPLICANT::varchar AS BILLOWNERORAPPLICANT, 
                        src:BUILDCONSENTDATE::datetime AS BUILDCONSENTDATE, 
                        src:BUILDCONSENTNO::varchar AS BUILDCONSENTNO, 
                        src:CT::varchar AS CT, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEVCONTPAID::varchar AS DEVCONTPAID, 
                        src:DISCONNECTIONREASON::varchar AS DISCONNECTIONREASON, 
                        src:DISCONNECTMETER::varchar AS DISCONNECTMETER, 
                        src:DP::varchar AS DP, 
                        src:ESTIMATEDDAILYWATERDEMAND::integer AS ESTIMATEDDAILYWATERDEMAND, 
                        src:ESTIMATEDPEAKFLOW::integer AS ESTIMATEDPEAKFLOW, 
                        src:FIRESPRINKLERMETER::varchar AS FIRESPRINKLERMETER, 
                        src:FLOORAREALT65::varchar AS FLOORAREALT65, 
                        src:INDIVIDUALPROPERTYTITLE::varchar AS INDIVIDUALPROPERTYTITLE, 
                        src:INDUSTRYTYPE::varchar AS INDUSTRYTYPE, 
                        src:INSTALLIN10DAYS::varchar AS INSTALLIN10DAYS, 
                        src:IRRIGATIONMETER::varchar AS IRRIGATIONMETER, 
                        src:LOT::varchar AS LOT, 
                        src:MAGFLOWPOWERTYPE::varchar AS MAGFLOWPOWERTYPE, 
                        src:MAGFLOWREQ::varchar AS MAGFLOWREQ, 
                        src:METERBOXSIZE::varchar AS METERBOXSIZE, 
                        src:METERNO::varchar AS METERNO, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NEIGHBOURHOOD::varchar AS NEIGHBOURHOOD, 
                        src:NEWMETERDIAMETER::integer AS NEWMETERDIAMETER, 
                        src:NEWWATERMETERLOC::varchar AS NEWWATERMETERLOC, 
                        src:NUMBEROFMETERS::integer AS NUMBEROFMETERS, 
                        src:PIPEMATERIAL::varchar AS PIPEMATERIAL, 
                        src:RELATIONTOPROPERTY::varchar AS RELATIONTOPROPERTY, 
                        src:RELOCATEMETER::varchar AS RELOCATEMETER, 
                        src:RELOCATIONTYPE::varchar AS RELOCATIONTYPE, 
                        src:REMOTEREADER::varchar AS REMOTEREADER, 
                        src:RESOURCECONSENTDATE::datetime AS RESOURCECONSENTDATE, 
                        src:RESOURCECONSENTNO::varchar AS RESOURCECONSENTNO, 
                        src:SHAREDTOSEPARATEMETER::varchar AS SHAREDTOSEPARATEMETER, 
                        src:SITECONNECTEDTOWASTEWATER::varchar AS SITECONNECTEDTOWASTEWATER, 
                        src:SITECONNECTEDTOWATER::varchar AS SITECONNECTEDTOWATER, 
                        src:SPRINKLERSUPPLYPIPESIZE::integer AS SPRINKLERSUPPLYPIPESIZE, 
                        src:STAGE::varchar AS STAGE, 
                        src:STRAINER::varchar AS STRAINER, 
                        src:SUPERLOT::varchar AS SUPERLOT, 
                        src:TENURE::varchar AS TENURE, 
                        src:VACPITNO::varchar AS VACPITNO, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WASTEWATERCONNSIZE::varchar AS WASTEWATERCONNSIZE, 
                        src:WASTEWATERCONTYPE::varchar AS WASTEWATERCONTYPE, 
                        src:WASTEWATERDISCONNECT::varchar AS WASTEWATERDISCONNECT, 
                        src:WASTEWATERINCREASECONNSIZE::varchar AS WASTEWATERINCREASECONNSIZE, 
                        src:WASTEWATERINCREASINGDEMAND::varchar AS WASTEWATERINCREASINGDEMAND, 
                        src:WASTEWATERINSTALLREQUIRED::varchar AS WASTEWATERINSTALLREQUIRED, 
                        src:WASTEWATERINSTALLTYPE::varchar AS WASTEWATERINSTALLTYPE, 
                        src:WASTEWATERNEWCONN::varchar AS WASTEWATERNEWCONN, 
                        src:WASTEWATERNEWCONNSIZE::varchar AS WASTEWATERNEWCONNSIZE, 
                        src:WASTEWATERNOCONNECTIONREQU::varchar AS WASTEWATERNOCONNECTIONREQU, 
                        src:WASTEWATERRELOCATION::varchar AS WASTEWATERRELOCATION, 
                        src:WASTEWATERUSEEXISTINGCONN::varchar AS WASTEWATERUSEEXISTINGCONN, 
                        src:WATERDISCONNECTMETER::varchar AS WATERDISCONNECTMETER, 
                        src:WATERINCREASEMETERSIZE::varchar AS WATERINCREASEMETERSIZE, 
                        src:WATERMAINLOC::varchar AS WATERMAINLOC, 
                        src:WATERMAINSIZE::integer AS WATERMAINSIZE, 
                        src:WATERNEWFIRESPRINKLERCONNE::varchar AS WATERNEWFIRESPRINKLERCONNE, 
                        src:WATERNEWIRRIAGTIONMETER::varchar AS WATERNEWIRRIAGTIONMETER, 
                        src:WATERNEWMETER::varchar AS WATERNEWMETER, 
                        src:WATERNEWMETERSIZE::varchar AS WATERNEWMETERSIZE, 
                        src:WATERNOCONNECTIONREQUIRED::varchar AS WATERNOCONNECTIONREQUIRED, 
                        src:WATERRELOCATEMETER::varchar AS WATERRELOCATEMETER, 
                        src:WATERSEPARATEASHAREDMETER::varchar AS WATERSEPARATEASHAREDMETER, 
                        src:WSLSTDDRAWING::varchar AS WSLSTDDRAWING, 
                        src:XBUILDDOEMSTICDETAILSKEY::integer AS XBUILDDOEMSTICDETAILSKEY, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:XBUILDDOEMSTICDETAILSKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_WSLCDRBUILD_XBUILDNONDOMDP as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:APBLDGAPPLDTLKEY), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:BUILDCONSENTDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESTIMATEDDAILYWATERDEMAND), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ESTIMATEDPEAKFLOW), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NEWMETERDIAMETER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:NUMBEROFMETERS), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:RESOURCECONSENTDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SPRINKLERSUPPLYPIPESIZE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WATERMAINSIZE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:XBUILDDOEMSTICDETAILSKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null