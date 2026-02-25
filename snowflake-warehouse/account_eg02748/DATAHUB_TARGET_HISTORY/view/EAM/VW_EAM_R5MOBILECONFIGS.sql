
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5MOBILECONFIGS
                   as                       
                    SELECT
                        t.MCF_CODE,
                        t.MCF_RCODE,
                        t.MCF_CONFIG,
                        t.MCF_DESK,
                        t.MCF_COMPTYPE,
                        t.MCF_USER,
                        t.MCF_GROUP,
                        t.MCF_XMLCONFIG,
                        t.MCF_CREATED,
                        t.MCF_UPDATED,
                        t.MCF_UPDATECOUNT,
                        t.MCF_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5MOBILECONFIGS as  t
					 union
					        SELECT
                        th.MCF_CODE,
                        th.MCF_RCODE,
                        th.MCF_CONFIG,
                        th.MCF_DESK,
                        th.MCF_COMPTYPE,
                        th.MCF_USER,
                        th.MCF_GROUP,
                        th.MCF_XMLCONFIG,
                        th.MCF_CREATED,
                        th.MCF_UPDATED,
                        th.MCF_UPDATECOUNT,
                        th.MCF_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5MOBILECONFIGS as  th ;
                     