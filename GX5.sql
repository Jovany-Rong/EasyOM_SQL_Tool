SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.1 业务信息表数据检查' 规则名称,
'DJF_GX_YWXX表XMZT与登记库不一致' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM BDC_xm C 
 where exists (
    select 1 from bdcdj_gx.djf_dj_ywxx y where y.ywh = c.proid and y.xmzt != c.xmzt
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.1 业务信息表数据检查' 规则名称,
'DJF_GX_YWXX表在登记库中无对应数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdcdj_gx.djf_dj_ywxx y 
 where not exists (
    select 1 from bdc_xm c where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.1 业务信息表数据检查' 规则名称,
'DJF_GX_YWXX表缺失的登记库办结数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdc_xm c 
 where c.xmzt = 1 and not exists (
    select 1 from bdcdj_gx.djf_dj_ywxx y where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.2 建设用地使用权表数据检查' 规则名称,
'QLF_QL_JSYDSYQ表QSZT与登记库不一致' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM BDC_jsydzjdsyq C 
 where exists (
    select 1 from bdcdj_gx.qlf_ql_jsydsyq y where y.ywh = c.proid and y.qszt != c.qszt
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.2 建设用地使用权表数据检查' 规则名称,
'QLF_QL_JSYDSYQ表在登记库中无对应数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdcdj_gx.qlf_ql_jsydsyq y 
 where not exists (
    select 1 from bdc_jsydzjdsyq c where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.2 建设用地使用权表数据检查' 规则名称,
'QLF_QL_JSYDSYQ表缺失的登记库现势数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdc_jsydzjdsyq c 
 where c.qszt = 1 and not exists (
    select 1 from bdcdj_gx.QLF_QL_JSYDSYQ y where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.3 项目内多幢房地产权表数据检查' 规则名称,
'QLT_FW_FDCQ_DZ表QSZT与登记库不一致' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM BDC_fdcq_dz C 
 where exists (
    select 1 from bdcdj_gx.qlt_fw_fdcq_dz y where y.ywh = c.proid and y.qszt != c.qszt
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.3 项目内多幢房地产权表数据检查' 规则名称,
'QLT_FW_FDCQ_DZ表在登记库中无对应数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdcdj_gx.QLT_FW_FDCQ_DZ y 
 where not exists (
    select 1 from bdc_fdcq_dz c where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.3 项目内多幢房地产权表数据检查' 规则名称,
'QLT_FW_FDCQ_DZ表缺失的登记库现势数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdc_fdcq_dz c 
 where c.qszt = 1 and not exists (
    select 1 from bdcdj_gx.QLT_FW_FDCQ_DZ y where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.4 房地产权表数据检查' 规则名称,
'QLT_FW_FDCQ_YZ表QSZT与登记库不一致' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM BDC_fdcq C 
 where exists (
    select 1 from bdcdj_gx.qlt_fw_fdcq_yz y where y.ywh = c.proid and y.qszt != c.qszt
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.4 房地产权表数据检查' 规则名称,
'QLT_FW_FDCQ_YZ表在登记库中无对应数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdcdj_gx.QLT_FW_FDCQ_YZ y 
 where not exists (
    select 1 from bdc_fdcq c where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.4 房地产权表数据检查' 规则名称,
'QLT_FW_FDCQ_YZ表缺失的登记库现势数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdc_fdcq c 
 where c.qszt = 1 and not exists (
    select 1 from bdcdj_gx.QLT_FW_FDCQ_YZ y where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.5 海域使用权表数据检查' 规则名称,
'QLF_QL_HYSYQ表QSZT与登记库不一致' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM BDC_jsydzjdsyq C 
 where exists (
    select 1 from bdcdj_gx.qlf_ql_hysyq y where y.ywh = c.proid and y.qszt != c.qszt
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.5 海域使用权表数据检查' 规则名称,
'QLF_QL_HYSYQ表在登记库中无对应数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdcdj_gx.QLF_QL_HYSYQ y 
 where not exists (
    select 1 from bdc_hysyq c where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.5 海域使用权表数据检查' 规则名称,
'QLF_QL_HYSYQ表缺失的登记库现势数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdc_hysyq c 
 where c.qszt = 1 and not exists (
    select 1 from bdcdj_gx.QLF_QL_HYSYQ y where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.6 抵押权表数据检查' 规则名称,
'QLF_QL_DYAQ表QSZT与登记库不一致' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM BDC_dyaq C 
 where exists (
    select 1 from bdcdj_gx.qlf_ql_dyaq y where y.ywh = c.proid and y.qszt != c.qszt
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.6 抵押权表数据检查' 规则名称,
'QLF_QL_DYAQ表在登记库中无对应数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdcdj_gx.QLF_QL_DYAQ y 
 where not exists (
    select 1 from bdc_dyaq c where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.6 抵押权表数据检查' 规则名称,
'QLF_QL_DYAQ表缺失的登记库现势数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdc_dyaq c 
 where c.qszt = 1 and not exists (
    select 1 from bdcdj_gx.QLF_QL_DYAQ y where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.7 预告登记表数据检查' 规则名称,
'QLF_QL_YGDJ表QSZT与登记库不一致' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM BDC_yg C 
 where exists (
    select 1 from bdcdj_gx.qlf_ql_ygdj y where y.ywh = c.proid and y.qszt != c.qszt
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.7 预告登记表数据检查' 规则名称,
'QLF_QL_YGDJ表在登记库中无对应数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdcdj_gx.QLF_QL_YGDJ y 
 where not exists (
    select 1 from bdc_yg c where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.7 预告登记表数据检查' 规则名称,
'QLF_QL_YGDJ表缺失的登记库现势数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdc_yg c 
 where c.qszt = 1 and not exists (
    select 1 from bdcdj_gx.QLF_QL_YGDJ y where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.8 异议登记表数据检查' 规则名称,
'QLF_QL_YYDJ表QSZT与登记库不一致' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM BDC_yy C 
 where exists (
    select 1 from bdcdj_gx.qlf_ql_yydj y where y.ywh = c.proid and y.qszt != c.qszt
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.8 异议登记表数据检查' 规则名称,
'QLF_QL_YYDJ表在登记库中无对应数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdcdj_gx.QLF_QL_YYDJ y 
 where not exists (
    select 1 from bdc_yy c where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.8 异议登记表数据检查' 规则名称,
'QLF_QL_YYDJ表缺失的登记库现势数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdc_yy c 
 where c.qszt = 1 and not exists (
    select 1 from bdcdj_gx.QLF_QL_YYDJ y where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.9 查封登记表数据检查' 规则名称,
'QLF_QL_CFDJ表QSZT与登记库不一致' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM BDC_cf C 
 where exists (
    select 1 from bdcdj_gx.qlf_ql_cfdj y where y.ywh = c.proid and y.qszt != c.qszt
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.9 查封登记表数据检查' 规则名称,
'QLF_QL_CFDJ表在登记库中无对应数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdcdj_gx.QLF_QL_CFDJ y 
 where not exists (
    select 1 from bdc_cf c where y.ywh = c.proid
 )
UNION ALL
SELECT '4.1 不动产共享数据检查' 检查项,
'4.1.9 查封登记表数据检查' 规则名称,
'QLF_QL_CFDJ表缺失的登记库现势数据' 规则内容,
'5' 严重性,
COUNT(1) 数据量 FROM bdc_cf c 
 where c.qszt = 1 and not exists (
    select 1 from bdcdj_gx.QLF_QL_CFDJ y where y.ywh = c.proid
 )
--UNION ALL