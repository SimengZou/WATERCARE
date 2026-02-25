CREATE OR REPLACE VIEW LANDING_ERROR.VW_STREAM_IPS_METERMANAGEMENT_WATER_WATERMETERANALYSIS_ERROR AS SELECT src, 'IPS_METERMANAGEMENT_WATER_WATERMETERANALYSIS' as TABLE_OBJECT, CASE WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) THEN
                    'ADDDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:ADDDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY), '0'), 38, 10) is null and 
                    src:ADDRESSKEY is not null) THEN
                    'ADDRESSKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY1), '0'), 38, 10) is null and 
                    src:ADDRESSKEY1 is not null) THEN
                    'ADDRESSKEY1 contains a non-numeric value : \'' || AS_VARCHAR(src:ADDRESSKEY1) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is null and 
                    src:BATCHKEY is not null) THEN
                    'BATCHKEY contains a non-numeric value : \'' || AS_VARCHAR(src:BATCHKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREBILLABLE), '0'), 38, 10) is null and 
                    src:COMPAREBILLABLE is not null) THEN
                    'COMPAREBILLABLE contains a non-numeric value : \'' || AS_VARCHAR(src:COMPAREBILLABLE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREHIGHBILLABLE), '0'), 38, 10) is null and 
                    src:COMPAREHIGHBILLABLE is not null) THEN
                    'COMPAREHIGHBILLABLE contains a non-numeric value : \'' || AS_VARCHAR(src:COMPAREHIGHBILLABLE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREHIGHREADING), '0'), 38, 10) is null and 
                    src:COMPAREHIGHREADING is not null) THEN
                    'COMPAREHIGHREADING contains a non-numeric value : \'' || AS_VARCHAR(src:COMPAREHIGHREADING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREHIGHUSAGE), '0'), 38, 10) is null and 
                    src:COMPAREHIGHUSAGE is not null) THEN
                    'COMPAREHIGHUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:COMPAREHIGHUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREOPTION), '0'), 38, 10) is null and 
                    src:COMPAREOPTION is not null) THEN
                    'COMPAREOPTION contains a non-numeric value : \'' || AS_VARCHAR(src:COMPAREOPTION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREREADING), '0'), 38, 10) is null and 
                    src:COMPAREREADING is not null) THEN
                    'COMPAREREADING contains a non-numeric value : \'' || AS_VARCHAR(src:COMPAREREADING) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPAREREADINGDATE), '1900-01-01')) is null and 
                    src:COMPAREREADINGDATE is not null) THEN
                    'COMPAREREADINGDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:COMPAREREADINGDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREREADINGKEY), '0'), 38, 10) is null and 
                    src:COMPAREREADINGKEY is not null) THEN
                    'COMPAREREADINGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:COMPAREREADINGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPARETOTALBILLABLEUSAGE), '0'), 38, 10) is null and 
                    src:COMPARETOTALBILLABLEUSAGE is not null) THEN
                    'COMPARETOTALBILLABLEUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:COMPARETOTALBILLABLEUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPARETOTALUSAGE), '0'), 38, 10) is null and 
                    src:COMPARETOTALUSAGE is not null) THEN
                    'COMPARETOTALUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:COMPARETOTALUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREUSAGE), '0'), 38, 10) is null and 
                    src:COMPAREUSAGE is not null) THEN
                    'COMPAREUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:COMPAREUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTBILLABLE), '0'), 38, 10) is null and 
                    src:CURRENTBILLABLE is not null) THEN
                    'CURRENTBILLABLE contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTBILLABLE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTCOMPKEY), '0'), 38, 10) is null and 
                    src:CURRENTCOMPKEY is not null) THEN
                    'CURRENTCOMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTCOMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTHIGHBILLABLE), '0'), 38, 10) is null and 
                    src:CURRENTHIGHBILLABLE is not null) THEN
                    'CURRENTHIGHBILLABLE contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTHIGHBILLABLE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTHIGHREADING), '0'), 38, 10) is null and 
                    src:CURRENTHIGHREADING is not null) THEN
                    'CURRENTHIGHREADING contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTHIGHREADING) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTHIGHUSAGE), '0'), 38, 10) is null and 
                    src:CURRENTHIGHUSAGE is not null) THEN
                    'CURRENTHIGHUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTHIGHUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTREADING), '0'), 38, 10) is null and 
                    src:CURRENTREADING is not null) THEN
                    'CURRENTREADING contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTREADING) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CURRENTREADINGDATE), '1900-01-01')) is null and 
                    src:CURRENTREADINGDATE is not null) THEN
                    'CURRENTREADINGDATE contains a non-timestamp value : \'' || AS_VARCHAR(src:CURRENTREADINGDATE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTREADINGKEY), '0'), 38, 10) is null and 
                    src:CURRENTREADINGKEY is not null) THEN
                    'CURRENTREADINGKEY contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTREADINGKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTTOTALBILLABLEUSAGE), '0'), 38, 10) is null and 
                    src:CURRENTTOTALBILLABLEUSAGE is not null) THEN
                    'CURRENTTOTALBILLABLEUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTTOTALBILLABLEUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTTOTALUSAGE), '0'), 38, 10) is null and 
                    src:CURRENTTOTALUSAGE is not null) THEN
                    'CURRENTTOTALUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTTOTALUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTUSAGE), '0'), 38, 10) is null and 
                    src:CURRENTUSAGE is not null) THEN
                    'CURRENTUSAGE contains a non-numeric value : \'' || AS_VARCHAR(src:CURRENTUSAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCEPTIONTYPE), '0'), 38, 10) is null and 
                    src:EXCEPTIONTYPE is not null) THEN
                    'EXCEPTIONTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:EXCEPTIONTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHMULTIPLIER), '0'), 38, 10) is null and 
                    src:HIGHMULTIPLIER is not null) THEN
                    'HIGHMULTIPLIER contains a non-numeric value : \'' || AS_VARCHAR(src:HIGHMULTIPLIER) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INDEXNO), '0'), 38, 10) is null and 
                    src:INDEXNO is not null) THEN
                    'INDEXNO contains a non-numeric value : \'' || AS_VARCHAR(src:INDEXNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOWMULTIPLIER), '0'), 38, 10) is null and 
                    src:LOWMULTIPLIER is not null) THEN
                    'LOWMULTIPLIER contains a non-numeric value : \'' || AS_VARCHAR(src:LOWMULTIPLIER) || '\'' WHEN 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) THEN
                    'MODDTTM contains a non-timestamp value : \'' || AS_VARCHAR(src:MODDTTM) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLDESTACCOUNTKEY), '0'), 38, 10) is null and 
                    src:OLDESTACCOUNTKEY is not null) THEN
                    'OLDESTACCOUNTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:OLDESTACCOUNTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTKEY), '0'), 38, 10) is null and 
                    src:PARENTKEY is not null) THEN
                    'PARENTKEY contains a non-numeric value : \'' || AS_VARCHAR(src:PARENTKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) THEN
                    'POSITION contains a non-numeric value : \'' || AS_VARCHAR(src:POSITION) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROBLEMCODESERVICEREQUESTNO), '0'), 38, 10) is null and 
                    src:PROBLEMCODESERVICEREQUESTNO is not null) THEN
                    'PROBLEMCODESERVICEREQUESTNO contains a non-numeric value : \'' || AS_VARCHAR(src:PROBLEMCODESERVICEREQUESTNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROBLEMCODEWORKORDERNO), '0'), 38, 10) is null and 
                    src:PROBLEMCODEWORKORDERNO is not null) THEN
                    'PROBLEMCODEWORKORDERNO contains a non-numeric value : \'' || AS_VARCHAR(src:PROBLEMCODEWORKORDERNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READERCODESERVICEREQUESTNO), '0'), 38, 10) is null and 
                    src:READERCODESERVICEREQUESTNO is not null) THEN
                    'READERCODESERVICEREQUESTNO contains a non-numeric value : \'' || AS_VARCHAR(src:READERCODESERVICEREQUESTNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READERCODEWORKORDERNO), '0'), 38, 10) is null and 
                    src:READERCODEWORKORDERNO is not null) THEN
                    'READERCODEWORKORDERNO contains a non-numeric value : \'' || AS_VARCHAR(src:READERCODEWORKORDERNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGCOMPKEY), '0'), 38, 10) is null and 
                    src:READINGCOMPKEY is not null) THEN
                    'READINGCOMPKEY contains a non-numeric value : \'' || AS_VARCHAR(src:READINGCOMPKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGTYPE), '0'), 38, 10) is null and 
                    src:READINGTYPE is not null) THEN
                    'READINGTYPE contains a non-numeric value : \'' || AS_VARCHAR(src:READINGTYPE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REREADSERVICEREQUESTNO), '0'), 38, 10) is null and 
                    src:REREADSERVICEREQUESTNO is not null) THEN
                    'REREADSERVICEREQUESTNO contains a non-numeric value : \'' || AS_VARCHAR(src:REREADSERVICEREQUESTNO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROUTEKEY), '0'), 38, 10) is null and 
                    src:ROUTEKEY is not null) THEN
                    'ROUTEKEY contains a non-numeric value : \'' || AS_VARCHAR(src:ROUTEKEY) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEQUENCENO), '0'), 38, 10) is null and 
                    src:SEQUENCENO is not null) THEN
                    'SEQUENCENO contains a non-numeric value : \'' || AS_VARCHAR(src:SEQUENCENO) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USAGEPERCENTAGE), '0'), 38, 10) is null and 
                    src:USAGEPERCENTAGE is not null) THEN
                    'USAGEPERCENTAGE contains a non-numeric value : \'' || AS_VARCHAR(src:USAGEPERCENTAGE) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) THEN
                    'VARIATION_ID contains a non-numeric value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' WHEN 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WATERMETERANALYSISKEY), '0'), 38, 10) is null and 
                    src:WATERMETERANALYSISKEY is not null) THEN
                    'WATERMETERANALYSISKEY contains a non-numeric value : \'' || AS_VARCHAR(src:WATERMETERANALYSISKEY) || '\'' WHEN 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null) THEN
                'VARIATION_ID contains a non-timestamp value : \'' || AS_VARCHAR(src:VARIATION_ID) || '\'' END as COMMENT,  CURRENT_TIMESTAMP() as ETL_LANDING_LOAD_DATETIME,
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
                                    
                src:WATERMETERANALYSISKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_WATERMETERANALYSIS as strm)
                WHERE
                ROWNUMBER=1) where 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is null and 
                    src:ADDDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY), '0'), 38, 10) is null and 
                    src:ADDRESSKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ADDRESSKEY1), '0'), 38, 10) is null and 
                    src:ADDRESSKEY1 is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is null and 
                    src:BATCHKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREBILLABLE), '0'), 38, 10) is null and 
                    src:COMPAREBILLABLE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREHIGHBILLABLE), '0'), 38, 10) is null and 
                    src:COMPAREHIGHBILLABLE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREHIGHREADING), '0'), 38, 10) is null and 
                    src:COMPAREHIGHREADING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREHIGHUSAGE), '0'), 38, 10) is null and 
                    src:COMPAREHIGHUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREOPTION), '0'), 38, 10) is null and 
                    src:COMPAREOPTION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREREADING), '0'), 38, 10) is null and 
                    src:COMPAREREADING is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:COMPAREREADINGDATE), '1900-01-01')) is null and 
                    src:COMPAREREADINGDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREREADINGKEY), '0'), 38, 10) is null and 
                    src:COMPAREREADINGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPARETOTALBILLABLEUSAGE), '0'), 38, 10) is null and 
                    src:COMPARETOTALBILLABLEUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPARETOTALUSAGE), '0'), 38, 10) is null and 
                    src:COMPARETOTALUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:COMPAREUSAGE), '0'), 38, 10) is null and 
                    src:COMPAREUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTBILLABLE), '0'), 38, 10) is null and 
                    src:CURRENTBILLABLE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTCOMPKEY), '0'), 38, 10) is null and 
                    src:CURRENTCOMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTHIGHBILLABLE), '0'), 38, 10) is null and 
                    src:CURRENTHIGHBILLABLE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTHIGHREADING), '0'), 38, 10) is null and 
                    src:CURRENTHIGHREADING is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTHIGHUSAGE), '0'), 38, 10) is null and 
                    src:CURRENTHIGHUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTREADING), '0'), 38, 10) is null and 
                    src:CURRENTREADING is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:CURRENTREADINGDATE), '1900-01-01')) is null and 
                    src:CURRENTREADINGDATE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTREADINGKEY), '0'), 38, 10) is null and 
                    src:CURRENTREADINGKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTTOTALBILLABLEUSAGE), '0'), 38, 10) is null and 
                    src:CURRENTTOTALBILLABLEUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTTOTALUSAGE), '0'), 38, 10) is null and 
                    src:CURRENTTOTALUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:CURRENTUSAGE), '0'), 38, 10) is null and 
                    src:CURRENTUSAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:EXCEPTIONTYPE), '0'), 38, 10) is null and 
                    src:EXCEPTIONTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:HIGHMULTIPLIER), '0'), 38, 10) is null and 
                    src:HIGHMULTIPLIER is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:INDEXNO), '0'), 38, 10) is null and 
                    src:INDEXNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:LOWMULTIPLIER), '0'), 38, 10) is null and 
                    src:LOWMULTIPLIER is not null) or 
                    (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is null and 
                    src:MODDTTM is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:OLDESTACCOUNTKEY), '0'), 38, 10) is null and 
                    src:OLDESTACCOUNTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PARENTKEY), '0'), 38, 10) is null and 
                    src:PARENTKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:POSITION), '0'), 38, 10) is null and 
                    src:POSITION is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROBLEMCODESERVICEREQUESTNO), '0'), 38, 10) is null and 
                    src:PROBLEMCODESERVICEREQUESTNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:PROBLEMCODEWORKORDERNO), '0'), 38, 10) is null and 
                    src:PROBLEMCODEWORKORDERNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READERCODESERVICEREQUESTNO), '0'), 38, 10) is null and 
                    src:READERCODESERVICEREQUESTNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READERCODEWORKORDERNO), '0'), 38, 10) is null and 
                    src:READERCODEWORKORDERNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGCOMPKEY), '0'), 38, 10) is null and 
                    src:READINGCOMPKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:READINGTYPE), '0'), 38, 10) is null and 
                    src:READINGTYPE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:REREADSERVICEREQUESTNO), '0'), 38, 10) is null and 
                    src:REREADSERVICEREQUESTNO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:ROUTEKEY), '0'), 38, 10) is null and 
                    src:ROUTEKEY is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:SEQUENCENO), '0'), 38, 10) is null and 
                    src:SEQUENCENO is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:USAGEPERCENTAGE), '0'), 38, 10) is null and 
                    src:USAGEPERCENTAGE is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is null and 
                    src:VARIATION_ID is not null) or 
                    (TRY_TO_NUMERIC(NVL(AS_VARCHAR(src:WATERMETERANALYSISKEY), '0'), 38, 10) is null and 
                    src:WATERMETERANALYSISKEY is not null) or 
                (TRY_TO_TIMESTAMP(NVL(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is null and 
                src:VARIATION_ID is not null)