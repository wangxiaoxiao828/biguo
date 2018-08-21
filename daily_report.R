rm(list=ls())
args<-commandArgs(T)
# args[1] 业务报表取1或0
# args[2] 风控报表取1或0
# args[3] 统计当天日期取1, 统计前一天取0

switch.yewu <- args[1]
switch.fengkong <- args[2]
switch.date.max <- args[3]

# switch.yewu <- '1'
# switch.fengkong <- '1'
# switch.date.max <- '1'



print(switch.yewu)
print(switch.fengkong)
print(switch.date.max)
if(switch.date.max=='1'){date.max=Sys.Date()}
if(switch.date.max=='0'){date.max=Sys.Date()-1}
print(date.max)

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
library(splitstackshape)

setwd("/data/temp/wangxx/biguo")
source("fetch_rawdata.R")
rj_code <- read.xlsx("/data/temp/wangxx/拒绝原因编码.xlsx",sheet=1)%>%data.table()
l <- fetch.rawdata()
data.all <- l[[1]]
data.borrower_risk <- l[[2]]
data.risk_list <- l[[3]]
data.sauron <- l[[4]]
data.four_ele <- l[[5]]
data.pay <- l[[6]]
data.pay.detail <- l[[7]]
data.confirmed <- l[[8]]

source("clean_rawdata.R")
source("create_wide_table.R")
#同盾
data.tongdun.sql <- clean.borrower_risk(data.borrower_risk = data.borrower_risk[create_at>='2018-08-14'])
#索伦
data.sauron.sql <- clean.sauron(data.sauron = data.sauron[create_at>='2018-08-14'])
#风险名单
#data.risk_list.sql <- clean.risk_list(data.risk_list = data.risk_list[create_at>='2018-08-14'])
#四要素

data.complete <- create.wide.table(l)

#生成业务表一 data.yewu.final.1: 日期	支付方式	申请人数	支付人数	支付申请笔数	通过人数	支付神券人数	支付神券笔数	总计
data.complete.temp <- data.complete
data.complete.temp[is.na(order_update_at)]$order_update_at <- data.complete.temp[is.na(order_update_at)]$order_create_at 
data.complete.temp[is.na(bank_auth_update_at)]$bank_auth_update_at <- data.complete.temp[is.na(bank_auth_update_at)]$bank_auth_create_at 
data.complete.temp <- data.complete.temp[,.(user_id,real_name,reserved_phone,idcard,card_no,order_no,pay_type,type,order_status,audit_status,audit_code,price,amount,date=order_update_at,bank_auth_update_at,product_name,ele_status,ele_create_at)]
data.complete.temp <- data.complete.temp[!is.na(order_no)]%>%unique()

data.complete.temp$type <- as.character(data.complete.temp$type)
data.complete.temp$type <- ifelse(data.complete.temp$type=='1','apply','shenquan')

data.yewu.1 <- data.complete.temp[,.(人数=uniqueN(user_id),笔数=uniqueN(order_no)),.(date,pay_type,type,order_status)]%>%dcast.data.table(date+pay_type~order_status+type, fun.aggregate = sum,value.var = c('人数','笔数'))

data.yewu.2 <- data.complete.temp[order_status==1][,.(总计=sum(price)),.(date,pay_type)][order(date)]

data.yewu.3 <- data.complete.temp[type=='apply'][audit_status==1][,.(date=min(date)),.(user_id,pay_type)][,.(通过人数=uniqueN(user_id)),.(date,pay_type)]

data.yewu.final.1 <- merge(data.yewu.1,data.yewu.2,by.x = c('date','pay_type'),by.y=c('date','pay_type'),all = TRUE)%>%merge(data.yewu.3,by.x = c('date','pay_type'),by.y=c('date','pay_type'),all = TRUE)
data.yewu.final.1 <- data.yewu.final.1[,.(日期=date, 支付方式=pay_type, 申请人数=人数_0_apply+人数_1_apply,支付人数=人数_1_apply,支付申请笔数=笔数_1_apply,通过人数,支付神券人数=人数_1_shenquan,支付神券笔数=笔数_1_shenquan,总计)]
data.yewu.final.1$总计 <- as.character(data.yewu.final.1$总计)
data.yewu.final.1[is.na(data.yewu.final.1)] <- 0
data.yewu.final.1 <- data.yewu.final.1[日期<=date.max]

#生成业务表二data.yewu.final.2 : 日期	支付申请笔数	支付神券笔数	机花花	金额	总计

data.yewu.4 <- data.complete.temp[order_status==1][,.(笔数=uniqueN(order_no)),.(date,type)]%>%dcast.data.table(date~type, fun.aggregate = sum,value.var = c('笔数'))

data.yewu.5 <- data.complete.temp[order_status==1][,.(总计=sum(price)),.(date)][order(date)]

data.yewu.6 <- data.complete.temp[!is.na(product_name)][ele_status==1][,.(user_id,product_name,date=ele_create_at)]%>%unique()%>%dcast.data.table(date~product_name,fun.aggregate = uniqueN,value.var = 'user_id')
data.yewu.6 <- data.yewu.6[,.(date,机花花,金额=机花花*150)]

data.yewu.final.2 <- merge(data.yewu.4,data.yewu.5,by.x = 'date',by.y = 'date',all = TRUE)%>%merge(data.yewu.6,by.x = 'date',by.y = 'date',all = TRUE)
data.yewu.final.2[is.na(data.yewu.final.2)] <- 0
data.yewu.final.2 <- data.yewu.final.2[,.(日期=date,支付申请笔数=apply,支付神券笔数=shenquan,机花花,金额,总计=as.character(总计+金额))]
data.yewu.final.2 <- data.yewu.final.2[日期<=date.max]
#生成业务表三data.yewu.final.3: product_name	name	mobile	idcard	bank_card	create_at	status

data.yewu.7 <- data.complete.temp[!is.na(product_name)][,.(product_name,real_name,reserved_phone,idcard,card_no,date=ele_create_at,ele_status)]%>%unique()
data.yewu.7$ele_status <- ifelse(data.yewu.7$ele_status==1,"已通过","未通过")

data.yewu.final.3 <- data.yewu.7
colnames(data.yewu.final.3) <- c('产品','姓名','手机号','身份证号','银行卡号','日期','状态')
data.yewu.final.3 <- data.yewu.final.3[日期==date.max]
write.xlsx(data.yewu.final.3,"甲方验证四要素名单.xlsx")
#生成业务表四data.yewu.final.4: status	type	real_name	card_no	issuing	reserved_phone	date_order


data.yewu.8 <- data.complete.temp[type=='shenquan'][,.(order_status,real_name,card_no,reserved_phone,date)]%>%unique()
data.yewu.8$order_status <- ifelse(data.yewu.8$order_status==1,"已购买","未购买")
data.yewu.final.4 <- data.yewu.8
data.yewu.final.4$type <- '神券'
colnames(data.yewu.final.4) <- c('购买状态','姓名','银行卡号','手机号','日期','神券')
data.yewu.final.4 <- data.yewu.final.4[日期==date.max]

HTML(data.yewu.final.1,file = "daily_report_yewu_1.html", append = FALSE, innerBorder = 1, Border = 1, row.names = FALSE)
HTML(data.yewu.final.2,file = "daily_report_yewu_1.html", append = TRUE, innerBorder = 1, Border = 1, row.names = FALSE)
if( nrow(data.yewu.final.3)!=0){
  HTML(data.yewu.final.3,file = "daily_report_yewu_2.html", append = FALSE, innerBorder = 1, Border = 1, row.names = FALSE)
}
if( nrow(data.yewu.final.3)==0){
  HTML('本日尚无查询四要素',file = "daily_report_yewu_2.html", append = FALSE, innerBorder = 1, Border = 1, row.names = FALSE)
}
if( nrow(data.yewu.final.4)!=0){
  HTML(data.yewu.final.4,file = "daily_report_yewu_2.html", append = TRUE, innerBorder = 1, Border = 1, row.names = FALSE)
}
if( nrow(data.yewu.final.4)==0){
  HTML('本日尚无支付神券行为',file = "daily_report_yewu_2.html", append = TRUE, innerBorder = 1, Border = 1, row.names = FALSE)
}

# 生成风控表一:日期	申请人数	支付人数	支付率	通过人数	通过率
data.complete.temp <- cSplit(data.complete.temp,'audit_code',direction = 'long')%>%unique()
data.complete.temp$audit_code <- as.character(data.complete.temp$audit_code)
data.complete.temp <- data.complete.temp[,.(audit_code=max(audit_code)),.(user_id,order_no,pay_type,type,order_status,audit_status,price,amount,date)]

data.fengkong.1 <- data.complete.temp[type=='apply'][,.(申请人数=uniqueN(user_id)),.(date)]
data.fengkong.2 <- data.complete.temp[type=='apply'][order_status==1][,.(支付人数=uniqueN(user_id)),.(date)]
data.fengkong.3 <- data.complete.temp[type=='apply'][order_status==1][audit_status==1][,.(通过人数=uniqueN(user_id)),.(date)]

data.fengkong.final.1 <- merge(data.fengkong.1,data.fengkong.2,all = TRUE)%>%merge(data.fengkong.3,all = TRUE)
data.fengkong.final.1 <- data.fengkong.final.1[,.(日期=date,申请人数,支付人数,支付率=支付人数/申请人数,通过人数,通过率=通过人数/支付人数)]
data.fengkong.final.1 <- data.fengkong.final.1[日期<=date.max]

HTML(data.fengkong.final.1,file = "daily_report_fengkong_1.html", append = FALSE, innerBorder = 1, Border = 1, row.names = FALSE)

data.plot <- data.complete.temp[,.(order_status=max(order_status),date=min(date)),.(user_id,audit_status,audit_code)][order_status==1] %>% unique()

data.plot[audit_status==2]$audit_code <- 'D000'
data.plot[audit_status==1]$audit_code <- 'approve'
data.plot[audit_status==-1][order_status==1]$audit_code <- 'D001'
data.plot <- data.plot[,.(user_id,audit_status,audit_code,date)]%>%unique()
data.exc <- data.plot



# 定义拒绝原因顺序,用于作瀑布图
rj.order <- rj_code[,.(audit_code=拒绝代码,type=接口,order=顺序)]
setorder(rj.order,order)
rj.order <- rbind(data.frame(audit_code='total',type='total',order=0),rj.order)%>%rbind(data.frame(audit_code='approve',type='approve',order=999))


a <- data.exc[date==date.max][,.(n=length(user_id)),.(audit_code)][!is.na(audit_code)]

# a为两列数据,audit_code和n 用于作瀑布图

a$n <- -a$n
temp.1 <- data.frame('total',-sum(a$n))
colnames(temp.1) <- colnames(a)
a <- rbind(a,temp.1)
a <- merge(a,rj.order)
a <- setorder(a,order)[,.(audit_code,n)]
a$abs <- abs(a$n)
waterfall.1 <- waterfall(.data=a[,.(audit_code,n)]
          ,rect_text_labels = paste(a$audit_code,'\n',a$abs)
          # ,calc_total = TRUE, total_rect_color = "darkslateblue"
          # ,total_rect_text = paste('After','\n',sum(mydata$data))
          )+labs(title = paste(date.max,"人数-拒绝原因"))+theme(plot.title = element_text(hjust = 0.5))

# 另一个瀑布图
b <- merge(a,rj.order,by.x = 'audit_code',by.y = 'audit_code')[,.(type,n,abs)]
b <- b[,.(n=sum(n),abs=sum(abs)),.(type)]
b <- merge(b,rj.order,by.x = 'type',by.y = 'type')
b <- setorder(b,order)[,-c('audit_code','order')]
b <- unique(b)
waterfall.2 <- waterfall(.data=b[,.(type,n)]
          ,rect_text_labels = paste(b$type,'\n',b$abs)
          # ,calc_total = TRUE, total_rect_color = "darkslateblue"
          # ,total_rect_text = paste('After','\n',sum(mydata$data))
          )+labs(title = paste(date.max,"人数-拒绝机构"))+theme(plot.title = element_text(hjust = 0.5))

# a.his用于作历史瀑布图
a.his <- data.exc[date<date.max][,.(n=length(user_id)),.(audit_code)][!is.na(audit_code)]
a.his$n <- -a.his$n
a.his <- rbind(a.his,data.frame(audit_code='total',n=-sum(a.his$n)))
a.his <- merge(a.his,rj.order,by.x = 'audit_code',by.y = 'audit_code')
a.his <- setorder(a.his,order)[,-c('type','order')]
a.his$abs <- abs(a.his$n)
waterfall.3 <- waterfall(.data=a.his[,.(audit_code,n)]
          ,rect_text_labels = paste(a.his$audit_code,'\n',a.his$abs)
          # ,calc_total = TRUE, total_rect_color = "darkslateblue"
          # ,total_rect_text = paste('After','\n',sum(mydata$data))
)+labs(title = "历史人数-拒绝原因")+theme(plot.title = element_text(hjust = 0.5))

# 另一个瀑布图
b.his <- merge(a.his,rj.order,by.x = 'audit_code',by.y = 'audit_code')[,.(type,n,abs)]
b.his <- b.his[,.(n=sum(n),abs=sum(abs)),.(type)]
b.his <- merge(b.his,rj.order,by.x = 'type',by.y = 'type')
b.his <- setorder(b.his,order)[,-c('audit_code','order')]
b.his <- unique(b.his)
waterfall.4 <- waterfall(.data=b.his[,.(type,n)]
          ,rect_text_labels = paste(b.his$type,'\n',b.his$abs)
          # ,calc_total = TRUE, total_rect_color = "darkslateblue"
          # ,total_rect_text = paste('After','\n',sum(mydata$data))
)+labs(title = "历史人数-拒绝机构")+theme(plot.title = element_text(hjust = 0.5))





# 作图并保存至daily_report.pdf
pdf("daily_report.pdf",family="GB1")
ggplot(data.fengkong.final.1, aes(x=日期, y=通过率, group=1)) + geom_line()+ labs(title = "通过率-日期")+theme(plot.title = element_text(hjust = 0.5))
waterfall.1
waterfall.2
waterfall.3
waterfall.4
dev.off()


# 拒绝原因分布
get.rj.distri <- function(a){
  rj.reason.distri <- a[,.(audit_code,abs)][!audit_code %in% c('total','approve')]
  rj.reason.distri <- rj.reason.distri[order(-abs)]
  rj.reason.distri$percent <- (rj.reason.distri$abs/sum(rj.reason.distri$abs))%>%substr(1,5)
  rj.reason <- merge(rj.reason.distri,rj.order,by.x='audit_code',by.y = 'audit_code')
  rj.reason.distri <- rj.reason[,.(拒绝来源=type,拒绝编码=audit_code,拒绝人数=abs,拒绝占比=percent)]
  
  rj.type.distri <- rj.reason[,.(abs=sum(abs)),.(type)]
  rj.type.distri$percent <- (rj.type.distri$abs/sum(rj.type.distri$abs))%>%substr(1,5)
  rj.type.distri <- rj.type.distri[order(-abs)][,.(拒绝来源=type,拒绝人数=abs,拒绝占比=percent)]
  
  
  rj.distri.1 <- dcast.data.table(rj.reason.distri, 拒绝来源~拒绝编码, value.var = '拒绝占比')
  rj.distri.1[is.na(rj.distri.1)] <- 0
  rj.distri.1$占比 <- 0;for (i in 1:nrow(rj.distri.1)){rj.distri.1$占比[i] <- sum(rj.distri.1[i,-1]%>%as.numeric())}
  rj.distri.1 <- rj.distri.1[order(-占比)]
  
  rj.distri.2 <- dcast.data.table(rj.reason.distri, 拒绝来源~拒绝编码, value.var = '拒绝人数')
  rj.distri.2[is.na(rj.distri.2)] <- 0
  rj.distri.2$占比 <- 0;for (i in 1:nrow(rj.distri.2)){rj.distri.2$占比[i] <- sum(rj.distri.2[i,-1]%>%as.numeric())}
  rj.distri.2 <- rj.distri.2[order(-占比)]
  return(list(rj.distri.1,rj.distri.2))
}
rj.distri.today <- get.rj.distri(a)
rj.distri.today.1 <- rj.distri.today[[1]]
rj.distri.today.2 <- rj.distri.today[[2]]
rj.distri.his <- get.rj.distri(a.his)
rj.distri.his.1 <- rj.distri.his[[1]]
rj.distri.his.2 <- rj.distri.his[[2]]

wb <- createWorkbook()

addWorksheet(wb, "当天拒绝原因占比分布")
addWorksheet(wb, "当天拒绝原因人数分布")
addWorksheet(wb, "历史拒绝原因占比分布")
addWorksheet(wb, "历史拒绝原因人数分布")

writeDataTable(wb, "当天拒绝原因占比分布", rj.distri.today.1)
writeDataTable(wb, "当天拒绝原因人数分布", rj.distri.today.2)
writeDataTable(wb, "历史拒绝原因占比分布", rj.distri.his.1)
writeDataTable(wb, "历史拒绝原因人数分布", rj.distri.his.2)

freezePane(wb, "当天拒绝原因占比分布", firstRow = TRUE)
freezePane(wb, "当天拒绝原因人数分布", firstRow = TRUE)
freezePane(wb, "历史拒绝原因占比分布", firstRow = TRUE)
freezePane(wb, "历史拒绝原因人数分布", firstRow = TRUE)

saveWorkbook(wb, paste0("日常报告",date.max,".xlsx"), overwrite = TRUE)

print(data.yewu.final.1)
print(data.yewu.final.2)
print(data.fengkong.final.1)
waterfall.1
waterfall.2
waterfall.3
waterfall.4

source("send_mail.R")


