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
  con <- dbConnect(MySQL(),host="47.96.167.191",dbname="db_coupon",user="gnh_read",password='T3D8[xSGqEhtYZs3')
  dbSendQuery(con,'set names gbk')
  # 借款人风险概要/同盾
  res <- dbSendQuery(con, iconv(paste0("select * from tb_borrower_risk_log where create_at >='2018-08-14';"),'CP936','UTF-8'))
  data.borrower_risk <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  data.borrower_risk$data <- iconv(data.borrower_risk$data,'CP936','UTF-8')
  # 用户总览
  res <- dbSendQuery(con, iconv(paste0("select * from tb_user_status where create_at >='2018-08-14';"),'CP936','UTF-8'))
  data.all <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  # 百度金融风险名单
  res <- dbSendQuery(con, iconv(paste0("select * from tb_risk_list_log where create_at >='2018-08-14';"),'CP936','UTF-8'))
  data.risk_list <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  data.risk_list$data <- iconv(data.risk_list$data,'CP936','UTF-8')
  # 运营商报告/索伦
  res <- dbSendQuery(con, iconv(paste0("select * from tb_operator_data where create_at >='2018-08-14';"),'CP936','UTF-8'))
  data.sauron <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  data.sauron$report_detail_data <- iconv(data.sauron$report_detail_data,'CP936','UTF-8')
  # 四要素报告
  res <- dbSendQuery(con, iconv(paste0("select * from tb_bank_card_auth;"),'CP936','UTF-8'))
  data.four_ele <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  data.four_ele$real_name <- iconv(data.four_ele$real_name,'CP936','UTF-8')
  data.four_ele$issuing <- iconv(data.four_ele$issuing,'CP936','UTF-8')
  # 1.99和50订单详情
  res <- dbSendQuery(con, iconv(paste0("select * from tb_orde_info where create_at >='2018-08-14 15:55:00';"),'CP936','UTF-8'))
  data.pay <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  # 付钱详情
  res <- dbSendQuery(con, iconv(paste0("select * from tb_pay_detail where create_at >='2018-08-14';"),'CP936','UTF-8'))
  data.pay.detail <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  # 甲方调用四要素接口
  res <- dbSendQuery(con, iconv(paste0("select * from tb_four_ele_log where create_at >='2018-08-15';"),'CP936','UTF-8'))
  data.confirmed <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  data.confirmed$name <- iconv(data.confirmed$name,'CP936','UTF-8')
  
  
  res <- dbSendQuery(con, iconv(paste0("select * from tb_black_list where mobile is not null;"),'CP936','UTF-8'))
  data.flag <- dbFetch(res, n= -1)%>%data.table()%>%unique()
  data.flag[is.na(type)]$type <- 'black'
  dbDisconnect(con);
  list(data.all,data.borrower_risk,data.risk_list,data.sauron,data.four_ele,data.pay,data.pay.detail,data.confirmed)
}



