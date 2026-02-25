
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DDFIELD
                   as                       
                    SELECT
                        t.DDF_FIELDID,
                        t.DDF_SOURCENAME,
                        t.DDF_DATATYPE,
                        t.DDF_DESC,
                        t.DDF_VALUEMAPID,
                        t.DDF_UPDATECOUNT,
                        t.DDF_RENTITY,
                        t.DDF_LVGRID,
                        t.DDF_TABLENAME,
                        t.DDF_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DDFIELD as  t
					 union
					        SELECT
                        th.DDF_FIELDID,
                        th.DDF_SOURCENAME,
                        th.DDF_DATATYPE,
                        th.DDF_DESC,
                        th.DDF_VALUEMAPID,
                        th.DDF_UPDATECOUNT,
                        th.DDF_RENTITY,
                        th.DDF_LVGRID,
                        th.DDF_TABLENAME,
                        th.DDF_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DDFIELD as  th ;
                     