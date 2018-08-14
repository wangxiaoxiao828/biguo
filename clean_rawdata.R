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

# 结构化同盾数据- 个人风险概要
clean.borrower_risk <- function(data.borrower_risk){
  data.borrower_risk <- data.borrower_risk[!is.na(data)]
  data.tongdun.sql <- NULL
  for (i in 1:nrow(data.borrower_risk)){
    b <- fromJSON(data.borrower_risk$data[i]%>%toString(),flatten = FALSE)$data
    b$user_id <- data.borrower_risk$user_id[i]
    b.excute <- function(b){
      # 取出比较麻烦的risk_items
      b1 <- b$risk_items
      b1.1 <- b1
      b1.1$item_detail <- NULL
      b2 <- b$risk_items$item_detail
      b2.1.colnames <- c('fraud_type','type','platform_count')
      b2.1.colnames <- b2.1.colnames[b2.1.colnames %in% colnames(b2)]
      b2.1 <- subset(b2,select=b2.1.colnames)
      if(is.null(b2.1$fraud_type)){b2.1$fraud_type <- ''}
      if(is.null(b2.1$type)){b2.1$type <- ''}
      if(is.null(b2.1$platform_count)){b2.1$platform_count <- ''}
      bb <- cbind(b1.1,b2.1)
      bb[is.na(bb)] <- ''
      bb$fraud_type <- gsub('、',",",bb$fraud_type)
      b$risk_items <- NULL
      bbb <- cbind(data.frame(b,stringsAsFactors = FALSE),bb)
      
      bbb <- cbind(data.borrower_risk[i],bbb)
      ;
      bbb
    }
    tryCatch(data.tongdun.sql <- rbind(data.tongdun.sql,b.excute(b)),
             error=function(e){print(paste(i,"此记录未处理"))}
    )
  }
  data.tongdun.sql <- data.table(data.tongdun.sql)%>%unique();
  data.tongdun.sql
}

# 结构化索伦数据
clean.sauron <- function(data.sauron){
  data.sauron <- data.sauron[!is.na(report_detail_data)]
  data.sauron.sql <- data.table()
  for (i in 1: nrow(data.sauron)){
    data.sauron.sql <- fromJSON(data.sauron$report_detail_data[i]%>%toString(),flatten = TRUE)$data %>% data.table()%>% rbind(data.sauron.sql)
  }
  data.sauron.sql <- cbind(data.sauron,data.sauron.sql)%>%unique();
  data.sauron.sql
}

# 结构化百度金融风险名单数据
clean.risk_list <- function(data.risk_list){
  data.risk_list <- data.risk_list[!is.na(data)]
  data.risk_list.sql <- data.table()
  for (i in 1: nrow(data.risk_list)){
    temp.1 <- fromJSON(data.risk_list$data[i]%>%toString(),flatten = TRUE)$data 
    if (is.list(temp.1)){
      temp.1$riskDetail <- NULL
      temp.2 <- temp.1 %>% data.frame(stringsAsFactors = FALSE)%>% data.table()
      temp.3 <- cbind(data.risk_list[i,-c('data')],temp.2)
      temp.3$data <- data.risk_list$data[i]
      data.risk_list.sql <- temp.3 %>% rbind(data.risk_list.sql)
    }
    
  }
  ;
  data.risk_list.sql
}




