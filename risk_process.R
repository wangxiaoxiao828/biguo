library(RMySQL)
library(data.table)
library(magrittr)
library(openxlsx)
library(R2HTML)
library(mailR)
library(bit64)
library(waterfalls)
library(ggplot2)
library(rlist)
library(splitstackshape)

# 个人风险概要检测
# D901,D105,D106,D107,D108,D110,D111,D112,D207,D208,D209,D210,D211检测
data.tongdun.sql.exe <- function(data.tongdun.result){
  # D901 risk_score>=80直接拒绝
  D901.check <- function(data.tongdun.result){
    data.tongdun.result$D901 <- 1
    data.tongdun.result[risk_score>=80]$D901 <- 0;
    data.tongdun.result
  }
  data.tongdun.result <- D901.check(data.tongdun.result)
  data.tongdun.result[,.(D901=min(D901)),.(mobile)][D901==1]$mobile%>%uniqueN()
  
  # D105 外部接口-借款人风险概要-命中欺诈高级灰名单
  D105.check <- function(data.tongdun.result){
    data.tongdun.result$D105 <- 1
    data.tongdun.result[grepl("高级灰名单",item_name)]$D105 <- 0;
    data.tongdun.result
  }
  data.tongdun.result <- D105.check(data.tongdun.result)
  data.tongdun.result[,.(D105=min(D105)),.(mobile)][D105==1]$mobile%>%uniqueN()
  
  # D106 外部接口-借款人风险概要-命中欺诈中级灰名单且命中3条以上规则
  D106.check <- function(data.tongdun.result){
    data.tongdun.result$D106 <- 1
    # temp.1 <- data.tongdun.result[fraud_type!=''][,.(fraud_type_n=strsplit(fraud_type%>%toString(),',')[[1]]%>%uniqueN(),item_name),.(mobile,D106)][fraud_type_n>=3][grepl("中级灰名单",item_name)]
    # temp.1$D106 <- 0
    # data.tongdun.result[mobile %in% temp.1$mobile]$D106 <- 0;
    temp.1 <- cSplit(data.tongdun.result,'fraud_type',direction = 'long')
    temp.2 <- temp.1[grepl("中级灰名单",item_name)][,.(fraud_type_n=uniqueN(fraud_type)),.(mobile)][fraud_type_n>=8]
    temp.2$D106 <- 0
    data.tongdun.result[mobile %in% temp.2$mobile]$D106 <- 0;
    data.tongdun.result
  }
  data.tongdun.result <- D106.check(data.tongdun.result)
  data.tongdun.result[,.(D106=min(D106)),.(mobile)][D106==1]$mobile%>%uniqueN()
  
  # D107 外部接口-借款人风险概要-命中欺诈中级灰名单且同时命中机构代办和伪冒风险
  D107.check <- function(data.tongdun.result){
    data.tongdun.result$D107 <- 1
    temp.1 <- data.tongdun.result[fraud_type!=''][,.(fraud_type_n=c('机构代办','伪冒风险')[c('机构代办','伪冒风险') %in% strsplit(fraud_type%>%toString(),',')[[1]]]%>%uniqueN(),item_name),.(mobile)][fraud_type_n==2][grepl("中级灰名单",item_name)]
    temp.1$D107 <- 0
    data.tongdun.result[mobile %in% temp.1$mobile]$D107 <- 0;
    data.tongdun.result
  }
  data.tongdun.result <- D107.check(data.tongdun.result)
  data.tongdun.result[,.(D107=min(D107)),.(mobile)][D107==1]$mobile%>%uniqueN()
  
  # D108 外部接口-借款人风险概要-命中欺诈中级灰名单且同时命中作弊行为
  D108.check <- function(data.tongdun.result){
    data.tongdun.result$D108 <- 1
    temp.1 <- data.tongdun.result[fraud_type!=''][,.(fraud_type_n=c('作弊行为')[c('作弊行为') %in% strsplit(fraud_type%>%toString(),',')[[1]]]%>%uniqueN(),item_name),.(mobile)][fraud_type_n==1][grepl("中级灰名单",item_name)]
    temp.1$D108 <- 0
    data.tongdun.result[mobile %in% temp.1$mobile]$D108 <- 0;
    data.tongdun.result
  }
  data.tongdun.result <- D108.check(data.tongdun.result)
  data.tongdun.result[,.(D108=min(D108)),.(mobile)][D108==1]$mobile%>%uniqueN()
  
  # D207 外部接口-借款人风险概要-命中黑名单
  D207.check <- function(data.tongdun.result){
    data.tongdun.result$D207 <- 1
    data.tongdun.result[grepl("黑名单",item_name)]$D207 <- 0;
    data.tongdun.result
  }
  data.tongdun.result <- D207.check(data.tongdun.result)
  data.tongdun.result[,.(D207=min(D207)),.(mobile)][D207==1]$mobile%>%uniqueN()
  
  # D110 外部接口-借款人风险概要-3个月内申请贷款次数<=1
  D110.check <- function(data.tongdun.result){
    data.tongdun.result$D110 <- 0
    data.tongdun.result[grepl("3个月内申请人在多个平台申请借款",item_name)][as.numeric(platform_count)>1]$D110 <- 1;
    data.tongdun.result
  }
  data.tongdun.result <- D110.check(data.tongdun.result)
  data.tongdun.result[,.(D110=max(D110)),.(mobile)][D110==1]$mobile%>%uniqueN()
  
  # D111 外部接口-借款人风险概要-12个月内次数>30且6个月内次数<6
  D111.check <- function(data.tongdun.result){
    data.tongdun.result$D111 <- 1
    temp.1 <- data.tongdun.result[grepl("6个月内申请人在多个平台申请借款",item_name)&as.numeric(platform_count)<6]
    temp.2 <- data.tongdun.result[grepl("12个月内申请人在多个平台申请借款",item_name)&as.numeric(platform_count)>30][mobile %in% temp.1$mobile]
    data.tongdun.result[mobile %in% temp.2$mobile]$D111 <- 0;
    data.tongdun.result
  }
  data.tongdun.result <- D111.check(data.tongdun.result)
  data.tongdun.result[,.(D111=min(D111)),.(mobile)][D111==1]$mobile%>%uniqueN()
  
  # D208 外部接口-借款人风险概要-命中高危地区
  D208.check <- function(data.tongdun.result){
    data.tongdun.result$D208 <- 1
    data.tongdun.result[grepl("身份证归属地位于高风险较为集中地区",item_name)]$D208 <- 0;
    data.tongdun.result
  }
  data.tongdun.result <- D208.check(data.tongdun.result)
  data.tongdun.result[,.(D208=min(D208)),.(mobile)][D208==1]$mobile%>%uniqueN()
  
  # D209 外部接口-借款人风险概要-命中法院失信名单
  D209.check <- function(data.tongdun.result){
    data.tongdun.result$D209 <- 1
    data.tongdun.result[grepl("命中法院失信名单",item_name)]$D209 <- 0;
    data.tongdun.result
  }
  data.tongdun.result <- D209.check(data.tongdun.result)
  data.tongdun.result[,.(D209=min(D209)),.(mobile)][D209==1]$mobile%>%uniqueN()
  
  # D210 外部接口-借款人风险概要-命中法院执行名单
  D210.check <- function(data.tongdun.result){
    data.tongdun.result$D210 <- 1
    data.tongdun.result[grepl("命中法院执行名单",item_name)]$D210 <- 0;
    data.tongdun.result
  }
  data.tongdun.result <- D210.check(data.tongdun.result)
  data.tongdun.result[,.(D210=min(D210)),.(mobile)][D210==1]$mobile%>%uniqueN()
  
  # D211 外部接口-借款人风险概要-命中法院结案名单
  D211.check <- function(data.tongdun.result){
    data.tongdun.result$D211 <- 1
    data.tongdun.result[grepl("命中法院结案名单",item_name)]$D211 <- 0;
    data.tongdun.result
  }
  data.tongdun.result <- D211.check(data.tongdun.result)
  data.tongdun.result[,.(D211=min(D211)),.(mobile)][D211==1]$mobile%>%uniqueN()
  return(data.tongdun.result)
  
}

data.tongdun.result <- data.tongdun.sql.exe(data.tongdun.sql)

# 总通过tongdun
data.tongdun.result.1 <- data.tongdun.result[,.(D901=min(D901),D105=min(D105),D106=min(D106),D107=min(D107),D108=min(D108),D110=max(D110),D111=min(D111),D207=min(D207),D208=min(D208),D209=min(D209),D210=min(D210),D211=min(D211)),.(user_id,mobile)]
data.tongdun.result.1$tongdun <- 0
data.tongdun.result.1[D105==1][D106==1][D107==1][D108==1][D207==1][D208==1][D209==1][D210==1][D211==1][D110==1][D111==1][D901==1]$tongdun <- 1

# 汇总分项通过数和总通过数
data.tongdun.result.sum <- lapply(data.tongdun.result.1[,-c(1,2)],function(x){sum(x)})%>%data.frame(stringsAsFactors = FALSE)
data.tongdun.result.sum <- cbind(data.frame(user_id='sum',mobile='sum'),data.tongdun.result.sum)
data.tongdun.result.percent <- lapply(data.tongdun.result.1[,-c(1,2)],function(x){as.character(sum(x)/length(x))%>%substr(1,5)})%>%data.frame(stringsAsFactors = FALSE)
data.tongdun.result.percent  <- cbind(data.frame(user_id='percent',mobile='percent'),data.tongdun.result.percent)

data.tongdun.result.summary <- rbind(data.tongdun.result.1,data.tongdun.result.sum)
data.tongdun.result.summary <- rbind(data.tongdun.result.summary,data.tongdun.result.percent)
