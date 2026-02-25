
                    
                create or replace view DATAHUB_TARGET_HISTORY.EAM_R5DUXLAYOUT
                   as                       
                    SELECT
                        t.DXL_USERGROUP,
                        t.DXL_PAGENAME,
                        t.DXL_ELEMENTID,
                        t.DXL_ELEMENTTYPE,
                        t.DXL_FIELDSIZE,
                        t.DXL_ATTRIBUTE,
                        t.DXL_SYSTEMATTRIBUTE,
                        t.DXL_PRESENTINJSP,
                        t.DXL_SECTION,
                        t.DXL_SECTIONPOSITION,
                        t.DXL_POSITIONINGROUP,
                        t.DXL_SECTIONLABEL,
                        t.DXL_NEWCARD,
                        t.DXL_FIELDINFO,
                        t.DXL_DEFAULTVALUE,
                        t.DXL_RADIOOPTIONS,
                        t.DXL_UPDATECOUNT,
                        t.DXL_LASTSAVED,
                        t.ETL_LASTSAVED,
                        t.ETL_DELETED,
                        t.etl_load_datetime,
                      t.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET.EAM_R5DUXLAYOUT as  t
					 union
					        SELECT
                        th.DXL_USERGROUP,
                        th.DXL_PAGENAME,
                        th.DXL_ELEMENTID,
                        th.DXL_ELEMENTTYPE,
                        th.DXL_FIELDSIZE,
                        th.DXL_ATTRIBUTE,
                        th.DXL_SYSTEMATTRIBUTE,
                        th.DXL_PRESENTINJSP,
                        th.DXL_SECTION,
                        th.DXL_SECTIONPOSITION,
                        th.DXL_POSITIONINGROUP,
                        th.DXL_SECTIONLABEL,
                        th.DXL_NEWCARD,
                        th.DXL_FIELDINFO,
                        th.DXL_DEFAULTVALUE,
                        th.DXL_RADIOOPTIONS,
                        th.DXL_UPDATECOUNT,
                        th.DXL_LASTSAVED,
                        th.ETL_LASTSAVED,
                        th.ETL_DELETED,
                        th.etl_load_datetime,
                      th.etl_load_metadatafilename
          
                     FROM DATAHUB_TARGET_HISTORY.EAM_DELETED_R5DUXLAYOUT as  th ;
                     