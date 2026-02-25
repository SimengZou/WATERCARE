CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_IPS_METERMANAGEMENT_WATER_WATERMETERANALYSIS AS SELECT
                        src:ADDBY::varchar AS ADDBY, 
                        src:ADDDTTM::datetime AS ADDDTTM, 
                        src:ADDRESS::varchar AS ADDRESS, 
                        src:ADDRESSKEY::integer AS ADDRESSKEY, 
                        src:ADDRESSKEY1::integer AS ADDRESSKEY1, 
                        src:APPROVE::varchar AS APPROVE, 
                        src:AUDITFLAG::varchar AS AUDITFLAG, 
                        src:BATCHKEY::integer AS BATCHKEY, 
                        src:COLORASSIGNMENTNEEDED::varchar AS COLORASSIGNMENTNEEDED, 
                        src:COMPAREBILLABLE::numeric(38, 10) AS COMPAREBILLABLE, 
                        src:COMPAREESTIMATEDFLAG::varchar AS COMPAREESTIMATEDFLAG, 
                        src:COMPAREHIGHBILLABLE::numeric(38, 10) AS COMPAREHIGHBILLABLE, 
                        src:COMPAREHIGHREADING::numeric(38, 10) AS COMPAREHIGHREADING, 
                        src:COMPAREHIGHUSAGE::numeric(38, 10) AS COMPAREHIGHUSAGE, 
                        src:COMPAREOPTION::integer AS COMPAREOPTION, 
                        src:COMPAREREADING::numeric(38, 10) AS COMPAREREADING, 
                        src:COMPAREREADINGDATE::datetime AS COMPAREREADINGDATE, 
                        src:COMPAREREADINGKEY::integer AS COMPAREREADINGKEY, 
                        src:COMPARETOTALBILLABLEUSAGE::numeric(38, 10) AS COMPARETOTALBILLABLEUSAGE, 
                        src:COMPARETOTALUSAGE::numeric(38, 10) AS COMPARETOTALUSAGE, 
                        src:COMPAREUSAGE::numeric(38, 10) AS COMPAREUSAGE, 
                        src:COMPAREUSAGEREADINGKEYS::varchar AS COMPAREUSAGEREADINGKEYS, 
                        src:CURRENTBILLABLE::numeric(38, 10) AS CURRENTBILLABLE, 
                        src:CURRENTCOMPKEY::integer AS CURRENTCOMPKEY, 
                        src:CURRENTESTIMATEDFLAG::varchar AS CURRENTESTIMATEDFLAG, 
                        src:CURRENTFIELDNOTES::varchar AS CURRENTFIELDNOTES, 
                        src:CURRENTHIGHBILLABLE::numeric(38, 10) AS CURRENTHIGHBILLABLE, 
                        src:CURRENTHIGHREADING::numeric(38, 10) AS CURRENTHIGHREADING, 
                        src:CURRENTHIGHUSAGE::numeric(38, 10) AS CURRENTHIGHUSAGE, 
                        src:CURRENTMETERID::varchar AS CURRENTMETERID, 
                        src:CURRENTREADING::numeric(38, 10) AS CURRENTREADING, 
                        src:CURRENTREADINGDATE::datetime AS CURRENTREADINGDATE, 
                        src:CURRENTREADINGKEY::integer AS CURRENTREADINGKEY, 
                        src:CURRENTTOTALBILLABLEUSAGE::numeric(38, 10) AS CURRENTTOTALBILLABLEUSAGE, 
                        src:CURRENTTOTALUSAGE::numeric(38, 10) AS CURRENTTOTALUSAGE, 
                        src:CURRENTUNITTYPE::varchar AS CURRENTUNITTYPE, 
                        src:CURRENTUSAGE::numeric(38, 10) AS CURRENTUSAGE, 
                        src:CURRENTUSAGEREADINGKEYS::varchar AS CURRENTUSAGEREADINGKEYS, 
                        src:DATALAKE_DELETED::boolean AS DATALAKE_DELETED, 
                        src:DEFAULTACTION::varchar AS DEFAULTACTION, 
                        src:EXCEPTION::varchar AS EXCEPTION, 
                        src:EXCEPTIONTYPE::integer AS EXCEPTIONTYPE, 
                        src:HIGHMULTIPLIER::numeric(38, 10) AS HIGHMULTIPLIER, 
                        src:INDEXNO::integer AS INDEXNO, 
                        src:ISPBCODESRVREQRESOLVED::varchar AS ISPBCODESRVREQRESOLVED, 
                        src:ISPBCODEWORESOLVED::varchar AS ISPBCODEWORESOLVED, 
                        src:ISPROBLEMCODEEXCEPTION::varchar AS ISPROBLEMCODEEXCEPTION, 
                        src:ISRDCODESRVREQRESOLVED::varchar AS ISRDCODESRVREQRESOLVED, 
                        src:ISRDCODEWORESOLVED::varchar AS ISRDCODEWORESOLVED, 
                        src:ISREADERCODEEXCEPTION::varchar AS ISREADERCODEEXCEPTION, 
                        src:ISREADINGAPPROVED::varchar AS ISREADINGAPPROVED, 
                        src:ISREREADRESOLVED::varchar AS ISREREADRESOLVED, 
                        src:KEYCOLUMN::varchar AS KEYCOLUMN, 
                        src:LOWMULTIPLIER::numeric(38, 10) AS LOWMULTIPLIER, 
                        src:MANUALREVIEWFLAG::varchar AS MANUALREVIEWFLAG, 
                        src:MODBY::varchar AS MODBY, 
                        src:MODDTTM::datetime AS MODDTTM, 
                        src:NOREADFLAG::varchar AS NOREADFLAG, 
                        src:OLDESTACCOUNTKEY::integer AS OLDESTACCOUNTKEY, 
                        src:OLDESTACCOUNTNO::varchar AS OLDESTACCOUNTNO, 
                        src:OTHERACTION::varchar AS OTHERACTION, 
                        src:PARENTKEY::integer AS PARENTKEY, 
                        src:POSITION::integer AS POSITION, 
                        src:PROBLEMCODE::varchar AS PROBLEMCODE, 
                        src:PROBLEMCODEBACKCOLOR::varchar AS PROBLEMCODEBACKCOLOR, 
                        src:PROBLEMCODESERVICEREQUESTNO::integer AS PROBLEMCODESERVICEREQUESTNO, 
                        src:PROBLEMCODESERVICEREQUESTTYPE::varchar AS PROBLEMCODESERVICEREQUESTTYPE, 
                        src:PROBLEMCODETEXTCOLOR::varchar AS PROBLEMCODETEXTCOLOR, 
                        src:PROBLEMCODEWORKORDERNO::integer AS PROBLEMCODEWORKORDERNO, 
                        src:PROBLEMCODEWORKORDERTYPE::varchar AS PROBLEMCODEWORKORDERTYPE, 
                        src:READERCODE::varchar AS READERCODE, 
                        src:READERCODEBACKCOLOR::varchar AS READERCODEBACKCOLOR, 
                        src:READERCODESERVICEREQUESTNO::integer AS READERCODESERVICEREQUESTNO, 
                        src:READERCODESERVICEREQUESTTYPE::varchar AS READERCODESERVICEREQUESTTYPE, 
                        src:READERCODETEXTCOLOR::varchar AS READERCODETEXTCOLOR, 
                        src:READERCODEWORKORDERNO::integer AS READERCODEWORKORDERNO, 
                        src:READERCODEWORKORDERTYPE::varchar AS READERCODEWORKORDERTYPE, 
                        src:READINGCOMPKEY::integer AS READINGCOMPKEY, 
                        src:READINGCYCLE::varchar AS READINGCYCLE, 
                        src:READINGMETERID::varchar AS READINGMETERID, 
                        src:READINGTYPE::integer AS READINGTYPE, 
                        src:READINGUNITTYPE::varchar AS READINGUNITTYPE, 
                        src:REREAD::varchar AS REREAD, 
                        src:REREADSERVICEREQUESTNO::integer AS REREADSERVICEREQUESTNO, 
                        src:ROUTEID::varchar AS ROUTEID, 
                        src:ROUTEKEY::integer AS ROUTEKEY, 
                        src:SEQUENCENO::integer AS SEQUENCENO, 
                        src:SERREQ::varchar AS SERREQ, 
                        src:USAGEPERCENTAGE::numeric(38, 10) AS USAGEPERCENTAGE, 
                        src:VARIATION_ID::integer AS VARIATION_ID, 
                        src:WATERMETERANALYSISKEY::integer AS WATERMETERANALYSISKEY, 
                        src:WORKORDER::varchar AS WORKORDER, src:VARIATION_ID::integer as ETL_SEQUENCE_NUMBER,
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
                                        
                src:WATERMETERANALYSISKEY  ORDER BY 
            src:VARIATION_ID desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_IPS_METERMANAGEMENT_WATER_WATERMETERANALYSIS as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:ADDDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRESSKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ADDRESSKEY1), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:BATCHKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPAREBILLABLE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPAREHIGHBILLABLE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPAREHIGHREADING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPAREHIGHUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPAREOPTION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPAREREADING), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:COMPAREREADINGDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPAREREADINGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPARETOTALBILLABLEUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPARETOTALUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:COMPAREUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTBILLABLE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTCOMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTHIGHBILLABLE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTHIGHREADING), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTHIGHUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTREADING), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:CURRENTREADINGDATE), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTREADINGKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTTOTALBILLABLEUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTTOTALUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:CURRENTUSAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:EXCEPTIONTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:HIGHMULTIPLIER), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:INDEXNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:LOWMULTIPLIER), '0'), 38, 10) is not null and 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:MODDTTM), '1900-01-01')) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:OLDESTACCOUNTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PARENTKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:POSITION), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROBLEMCODESERVICEREQUESTNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:PROBLEMCODEWORKORDERNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READERCODESERVICEREQUESTNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READERCODEWORKORDERNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READINGCOMPKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:READINGTYPE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:REREADSERVICEREQUESTNO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:ROUTEKEY), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:SEQUENCENO), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:USAGEPERCENTAGE), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:VARIATION_ID), '0'), 38, 10) is not null and 
                    TRY_TO_NUMERIC(nvl(AS_VARCHAR(src:WATERMETERANALYSISKEY), '0'), 38, 10) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:VARIATION_ID), '1900-01-01')) is not null