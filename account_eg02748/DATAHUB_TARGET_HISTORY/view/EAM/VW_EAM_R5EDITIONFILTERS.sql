
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5EDITIONFILTERS
                   as                       
                    SELECT
                        t.EDF_CODE,
                        t.EDF_TYPE,
                        t.EDF_MEFLAG,
                        t.EDF_FILTER,
                        t.EDF_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5EDITIONFILTERS as  t
					 union
					        SELECT
                        th.EDF_CODE,
                        th.EDF_TYPE,
                        th.EDF_MEFLAG,
                        th.EDF_FILTER,
                        th.EDF_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5EDITIONFILTERS as  th ;
                     