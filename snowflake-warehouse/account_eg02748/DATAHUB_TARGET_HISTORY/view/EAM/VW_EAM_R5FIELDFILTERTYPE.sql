
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FIELDFILTERTYPE
                   as                       
                    SELECT
                        t.FFT_FUNCTION,
                        t.FFT_TYPE,
                        t.FFT_RTYPE,
                        t.FFT_DEFAULTSCREENTYPE,
                        t.FFT_UPDATECOUNT,
                        t.FFT_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FIELDFILTERTYPE as  t
					 union
					        SELECT
                        th.FFT_FUNCTION,
                        th.FFT_TYPE,
                        th.FFT_RTYPE,
                        th.FFT_DEFAULTSCREENTYPE,
                        th.FFT_UPDATECOUNT,
                        th.FFT_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FIELDFILTERTYPE as  th ;
                     