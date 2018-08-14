library(RMySQL)
library(data.table)
library(magrittr)
library(openxlsx)
library(R2HTML)
library(mailR)
library(bit64)
library(waterfalls)
library(ggplot2)
library(jsonlite)

source("fetch_rawdata.R")
l <- fetch.rawdata()
data.all <- l[[1]]
data.borrower_risk <- l[[2]]
data.risk_list <- l[[3]]
data.sauron <- l[[4]]
data.four_ele <- l[[5]]
data.pay <- l[[6]]

source("clean_rawdata.R")
#同盾
data.tongdun.sql <- clean.borrower_risk(data.borrower_risk = data.borrower_risk)
#索伦
data.sauron.sql <- clean.sauron(data.sauron = data.sauron)
#风险名单
data.risk_list.sql <- clean.risk_list(data.risk_list = data.risk_list)
#四要素


#处理总览
data.result <- data.all[black_list_status==0][borrower_risk_status==1][operator_report_status==1]
data.result <- data.all[audit_code=='']

rj_code <- read.xlsx("/data/temp/wangxx/拒绝原因编码.xlsx",sheet=1)%>%data.table()
data.exc <- data.all[,.(user_id,audit_code,update_at)]%>%unique()
data.exc$date <- substr(data.exc$update_at,1,10)
data.exc <- data.exc[date=='2018-08-08']

