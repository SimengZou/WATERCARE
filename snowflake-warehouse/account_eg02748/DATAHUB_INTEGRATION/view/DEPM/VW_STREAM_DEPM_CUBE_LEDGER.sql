CREATE OR REPLACE VIEW DATAHUB_INTEGRATION.VW_STREAM_DEPM_CUBE_LEDGER AS SELECT
                        src:"Account"::varchar AS "Account", 
                        src:Analysis01::varchar AS Analysis01, 
                        src:Analysis02::varchar AS Analysis02, 
                        src:Analysis03::varchar AS Analysis03, 
                        src:Analysis04::varchar AS Analysis04, 
                        src:Analysis05::varchar AS Analysis05, 
                        src:Analysis06::varchar AS Analysis06, 
                        src:Analysis07::varchar AS Analysis07, 
                        src:Analysis08::varchar AS Analysis08, 
                        src:Analysis09::varchar AS Analysis09, 
                        src:Analysis10::varchar AS Analysis10, 
                        src:Currency::varchar AS Currency, 
                        src:LdgTrxType::varchar AS LdgTrxType, 
                        src:LedgerMeas::varchar AS LedgerMeas, 
                        src:Period::varchar AS Period, 
                        src:Timestamp::datetime AS Timestamp, 
                        src:Value::varchar AS Value, 
                        src:Version::varchar AS Version, 
                        src:Year::varchar AS Year, 
                        src:Timestamp::datetime as ETL_LASTSAVED,etl_load_datetime, etl_load_metadatafilename FROM (SELECT * FROM ( SELECT
                src,
                etl_load_datetime,
                etl_load_metadatafilename,
                ROW_NUMBER() OVER (PARTITION BY 
                                        
                    src:Analysis09,
                    src:Analysis02,
                    src:Currency,
                    src:Analysis04,
                    src:Period,
                    src:Analysis07,
                    src:Analysis01,
                    src:Analysis10,
                    src:Analysis05,
                    src:Version,
                    src:Account,
                    src:LedgerMeas,
                    src:Analysis08,
                    src:Analysis03,
                    src:LdgTrxType,
                    src:Analysis06,
                    src:Year  ORDER BY src:Timestamp desc, src:Value desc,IFNULL(TRY_TO_TIMESTAMP(replace(right(replace(lower(etl_load_metadatafilename),'.json'),23),'_','-'), 'yyyy-mm-dd-HH-MI-SS-FF') ,etl_load_datetime) desc) as ROWNUMBER
                FROM DATAHUB_INTEGRATION.STREAM_DEPM_CUBE_LEDGER as strm)
                WHERE
                ROWNUMBER=1) where 
                    TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null and 
                TRY_TO_TIMESTAMP(nvl(AS_VARCHAR(src:Timestamp), '1900-01-01')) is not null