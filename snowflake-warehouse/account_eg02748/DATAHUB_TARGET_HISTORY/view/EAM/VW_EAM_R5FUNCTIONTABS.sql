
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5FUNCTIONTABS
                   as                       
                    SELECT
                        t.FTB_FUNCTION,
                        t.FTB_TAB,
                        t.FTB_RENTITY,
                        t.FTB_VISIBLE,
                        t.FTB_SELECT,
                        t.FTB_UPDATE,
                        t.FTB_INSERT,
                        t.FTB_DELETE,
                        t.FTB_DISPLAYFT,
                        t.FTB_SYSREQUIRED,
                        t.FTB_SEQ,
                        t.FTB_SQLEXIST,
                        t.FTB_UPDATECOUNT,
                        t.FTB_ALTERSAVE,
                        t.FTB_INTERFACECODE,
                        t.FTB_MEKEY,
                        t.FTB_EWSBTN,
                        t.FTB_XTYPE,
                        t.FTB_NODESIGN,
                        t.FTB_DESIGNPOPUP,
                        t.FTB_LASTSAVED,
                        t.FTB_PRODUCT,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5FUNCTIONTABS as  t
					 union
					        SELECT
                        th.FTB_FUNCTION,
                        th.FTB_TAB,
                        th.FTB_RENTITY,
                        th.FTB_VISIBLE,
                        th.FTB_SELECT,
                        th.FTB_UPDATE,
                        th.FTB_INSERT,
                        th.FTB_DELETE,
                        th.FTB_DISPLAYFT,
                        th.FTB_SYSREQUIRED,
                        th.FTB_SEQ,
                        th.FTB_SQLEXIST,
                        th.FTB_UPDATECOUNT,
                        th.FTB_ALTERSAVE,
                        th.FTB_INTERFACECODE,
                        th.FTB_MEKEY,
                        th.FTB_EWSBTN,
                        th.FTB_XTYPE,
                        th.FTB_NODESIGN,
                        th.FTB_DESIGNPOPUP,
                        th.FTB_LASTSAVED,
                        th.FTB_PRODUCT,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5FUNCTIONTABS as  th ;
                     