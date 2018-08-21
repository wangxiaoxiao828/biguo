library(RMySQL)
library(data.table)
library(magrittr)
library(openxlsx)
library(R2HTML)
library(mailR)
library(bit64)
library(waterfalls)
library(ggplot2)
# 从服务器取出当天及历史数据

fetch.rawdata <- function(){
  # con <- dbConnect(MySQL(),host="47.96.167.191",dbname="db_coupon",user="guannihua",password='T3D8{xSGqEhtYZs2')
  con <- dbConnect(MySQL(),host="47.96.167.191",dbname="db_coupon",user="gnh_read",password='T3D8[xSGqEhtYZs3',port=3308)
  dbSendQuery(con,'set names gbk')
  # 借款人风险概要/同盾
  res <- dbSendQuery(con, iconv(paste0("select * from tb_borrower_risk_log where create_at >='2018-08-14';"),'CP936','UTF-8'))
  tb_borrower_risk_log <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  tb_borrower_risk_log$data <- iconv(tb_borrower_risk_log$data,'CP936','UTF-8')
  # 用户总览
  res <- dbSendQuery(con, iconv(paste0("select * from tb_user_status where create_at >='2018-08-14';"),'CP936','UTF-8'))
  tb_user_status <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  # 百度金融风险名单
  res <- dbSendQuery(con, iconv(paste0("select * from tb_risk_list_log where create_at >='2018-08-14';"),'CP936','UTF-8'))
  tb_risk_list_log <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  tb_risk_list_log$data <- iconv(tb_risk_list_log$data,'CP936','UTF-8')
  # 运营商报告/索伦
  res <- dbSendQuery(con, iconv(paste0("select * from tb_operator_data where create_at >='2018-08-14';"),'CP936','UTF-8'))
  tb_operator_data <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  tb_operator_data$report_detail_data <- iconv(tb_operator_data$report_detail_data,'CP936','UTF-8')
  # 四要素报告
  res <- dbSendQuery(con, iconv(paste0("select * from tb_bank_card_auth;"),'CP936','UTF-8'))
  tb_bank_card_auth <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  tb_bank_card_auth$real_name <- iconv(tb_bank_card_auth$real_name,'CP936','UTF-8')
  tb_bank_card_auth$issuing <- iconv(tb_bank_card_auth$issuing,'CP936','UTF-8')
  # 1.99和50订单详情
  res <- dbSendQuery(con, iconv(paste0("select * from tb_orde_info where create_at >='2018-08-14 15:55:00';"),'CP936','UTF-8'))
  tb_orde_info <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  # 付钱详情
  res <- dbSendQuery(con, iconv(paste0("select * from tb_pay_detail where create_at >='2018-08-14';"),'CP936','UTF-8'))
  tb_pay_detail <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  # 甲方调用四要素接口
  res <- dbSendQuery(con, iconv(paste0("select * from tb_four_ele_log where create_at >='2018-08-15';"),'CP936','UTF-8'))
  tb_four_ele_log <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  tb_four_ele_log$name <- iconv(tb_four_ele_log$name,'CP936','UTF-8')
  
  dbDisconnect(con);
  list(tb_user_status,tb_borrower_risk_log,tb_risk_list_log,tb_operator_data,tb_bank_card_auth,tb_orde_info,tb_pay_detail,tb_four_ele_log)
}