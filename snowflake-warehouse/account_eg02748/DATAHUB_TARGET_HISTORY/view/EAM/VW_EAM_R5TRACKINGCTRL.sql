
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5TRACKINGCTRL
                   as                       
                    SELECT
                        t.TKC_INTERFACETYPE,
                        t.TKC_INTERFACERTYPE,
                        t.TKC_COLUMN,
                        t.TKC_RCOLUMN,
                        t.TKC_UPLOADCOLUMN,
                        t.TKC_DISPLAYSEQ,
                        t.TKC_LENGTH,
                        t.TKC_DATATYPE,
                        t.TKC_DATARTYPE,
                        t.TKC_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5TRACKINGCTRL as  t
					 union
					        SELECT
                        th.TKC_INTERFACETYPE,
                        th.TKC_INTERFACERTYPE,
                        th.TKC_COLUMN,
                        th.TKC_RCOLUMN,
                        th.TKC_UPLOADCOLUMN,
                        th.TKC_DISPLAYSEQ,
                        th.TKC_LENGTH,
                        th.TKC_DATATYPE,
                        th.TKC_DATARTYPE,
                        th.TKC_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5TRACKINGCTRL as  th ;
                     