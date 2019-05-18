
#' Function to standardise a variable with respect to an index position
#' (e.g. each time period) while in panel format.
#'
#' @param df data.frame in long (panel) format.
#' @param var_name Column name to standardise (string).
#' @param index_name Column name of index to standardise within (string).
#' @param type Standardisation method (string). Default="standardise".
#' @param vector_only Return standardised values only or full data.frame?
#' (Logical)
#' @details standardise = (x-mean(x))/sd(x).
#' min_max = (x-min(x))/(max(x)-min(x))
standardise_long <- function(df, var_name, index_name,
                             type=c("standardise", "min_max"),
                             vector_only=FALSE){
  type = match.arg(type)
  orig_columns <- colnames(df)
  colnames(df)[which(colnames(df)==var_name)] <- "var_tm90"
  colnames(df)[which(colnames(df)==index_name)] <- "ind_tm90"
  if(type=="standardise"){
    df_sum <- dplyr::group_by(df,ind_tm90) %>%
      dplyr::summarise(var_sd=sd(var_tm90, na.rm=T),
                       var_mean=mean(var_tm90, na.rm=T))
    df <- merge(df,df_sum,all.x = T) %>%
      dplyr::mutate(var_std=(var_tm90-var_mean)/var_sd)
  }
  if(type=="min_max"){
    df_sum <- dplyr::group_by(df,ind_tm90) %>%
      dplyr::summarise(var_min=min(var_tm90, na.rm=T),
                       var_range=diff(range(var_tm90, na.rm=T)))
    df <- merge(df,df_sum,all.x = T) %>%
      dplyr::mutate(var_std=(var_tm90-var_min)/var_range)
  }
  if(vector_only){
    return(df$var_std)
  }
  colnames(df)[which(colnames(df)=="var_tm90")] <- var_name
  colnames(df)[which(colnames(df)=="ind_tm90")] <- index_name
  colnames(df)[which(colnames(df)=="var_std")] <- paste0(var_name,"_sx")
  df <- df[,c(orig_columns, paste0(var_name,"_sx"))]
  return(df)
}

#' Function to standardise each *row* of a variable while in wide format.
#'
#' @param df An xts or data.frame in long (panel) format.
#' @param type Standardisation method (string). Default="standardise".
#' @details standardise = (x-mean(x))/sd(x).
#' min_max = (x-min(x))/(max(x)-min(x))
standardise_wide <- function(df, type=c("standardise", "min_max")){
  type = match.arg(type)
  xts_flag = FALSE
  if(match("xts",class(df))==1){
    ind <- zoo::index(df)
    xts_flag = TRUE
  }
  na_cols <- which(rowSums(is.na(df))==ncol(df))
  df[na_cols,1:min(10,ncol(df))] <- 1:min(10,ncol(df))
  if(type=="min_max"){
    df_std <- t(
      apply(df, 1, function(x) (x-min(x,na.rm = T))/diff(range(x,na.rm = T))))
  }
  if(type=="standardise"){
    df_std <- t(apply(df,1,function(x) (x-mean(x,na.rm = T))/sd(x,na.rm = T)))
  }
  df_std[na_cols,] <- NA
  if(xts_flag){
    df_std <- xts::xts(df_std,ind)
  }
  return(df_std)
}

#' Function to read csv files as xts objects.
#'
#' @param dir The directory in which the required file is stored (string)
#' @param fname name of file to be loaded (string)
#' @param index.col Column name to be referenced for ordering xts object
#' (string). Must be coercible to POSIXct.
#' @param daterange The date range of resulting xts to be loaded (string).
#' @return An xts object.
#' @export
readxts2 <- function(dir, fname, index.col="Date", daterange=NULL, delim=","){
  x <- read_delim(file.path(dir,fname), delim=delim)
  index.col <- names(x)[grep(index.col,names(x),ignore.case = TRUE)]
  #x[[index.col]] <- as.POSIXct(x[[index.col]])
  x<-as.xts(x[,which(names(x) != index.col)],order.by=x[[index.col]])
  #indexFormat(x)<-"%Y-%m-%d"
  if(!is.null(daterange)){
    x <- x[daterange]
  }
  return(x)
}

#' Function to read csv files and load them as xts into selected environment.
#' @description  Function to load multiple csv files into the selected environment as xts
#' objects. Object names are the file names, minus the '.csv' extension.
#' @param dir The directory in which the required files are stored (string)
#' @param fname name of file to be loaded (string)
#' @param index.col Column name to be referenced for ordering xts object
#' (string). Must be coercible to POSIXct.
#' @param daterange The date range of resulting xts to be loaded (string).
#' @param envir The environment in which to load the objects.
#' @return NULL
#' @export
load_files <- function(dir, fnames, index.col="date", daterange=NULL,
                      envir=globalenv(), delim=","){
  for (i in 1:length(fnames)){
    x <- readxts2(dir,fnames[i], index.col = index.col,
                  daterange=daterange, delim=delim)
    symbol <- gsub('\\..*', "", fnames[i])
    symbol <- gsub(" ", "", symbol)
    assign(symbol, x, envir=envir)
  }
}

#' Function to clean individual asset OHLC data.
#' @description Function to clean individual asset OHLC data. Changes column
#' names, and provides option to use adjusted data column to weight OHLC prices.
#' Silently drops all columns except for OHLC and Member (if Member=TRUE).
#' @param symbolList List of symbols loaded into the environment.
#' @param totalReturn If TRUE, will look for column of form "total_return", and
#' will use for adjustment.
#' @param Member If TRUE, will preserve membership column.
clean_names <- function(symbolList, totalReturn=FALSE,Member=FALSE,ind=NULL,
                        envir=globalenv()){
  # Assumes OHLC data with total return in 5th column
  for (i in 1:length(symbolList)) {
    symbol <- symbolList[i]
    x <- get(symbolList[i], envir=envir)
    if(!is.null(ind)){x <- cbind(x,ind)}
    # Close is usually available further back than OHL, OHL
    # with close until open is available
    x[which(is.na(x[,1])),1:3] <- x[which(is.na(x[,1])),4]
    #x[1:(which(!is.na(x[,1]))[1]-1),1:3] <- x[1:(which(!is.na(x[,1]))[1]-1),4]
    if(totalReturn){
      tr <- which(grepl("tot",names(x),ignore.case = TRUE) &
                    grepl("return",names(x),ignore.case = TRUE))
      if(length(tr)==0){
        warning("Total return column not found. Using 5th column.")
        tr=5}
      # Replace close by total return and adjust OHL by same amount
      r <- x[,tr]/x[,4]
      x[,4] <- x[,tr]
      x[,1:3] <- x[,1:3]*as.numeric(r)
    }
    if (Member){
      mem <- which(tolower(names(x))=="member")
      x <- x[,c(1:4,mem)]
      names(x) <- c("Open","High","Low","Close","Member")
    } else {
      x <- x[,1:4]
      names(x) <- c("Open","High","Low","Close")
    }
    assign(symbol,x,envir=envir)
  }
}

#' Function to replace NAs on last trading day of month with
#' value from previous trading day.
#' @description Function to replace NAs on last trading day of month with
#' value from previous or next trading day, up to `ndays`. Only replaces
#' data on endpoints (option for start points). Within-period NAs will not be
#' filled.
#' @param df xts object containing price series of assets to rank. Every column
#' will be treated as an independent asset.
#' @param firstDay If TRUE, replaces NAs on first trading day of month.
#' @param fromNext If True, replaces NAs with value from next business day.
#' @param ndays The maximum number of days from NA from which to find data.
#' @note NEED TO REMOVE Automatic NYSE trading days.
#'@export
naFillFrom <- function(df, firstDay=FALSE, fromNext=FALSE, ndays=5, ind=NULL,
                       on="months"){
  k=1
  if (fromNext){k=-1}
  if (is.null(ind)){
    ind <- nyse_trading_dates(as.Date(index(df)[1]),
                              as.Date(index(df)[nrow(df)]))
  } else{
    ind <- index(df)
    ind <- ind[!((weekdays(ind) == "Saturday") | (weekdays(ind) == "Sunday"))]
    ind <- ind[!(as.Date(ind) %in% as.Date(timeDate::holidayNYSE(
      lubridate::year(index(df))[1]:lubridate::year(index(df))[nrow(df)])))]
  }
  eps <- endpoints(ind,"months")[-1]
  if (firstDay){eps <- eps+1}
  eps_dates <- ind[eps]
  for (i in 1:ncol(df)){
    x <- df[,i]
    for (j in 1:ndays){
      na_eps <- which(is.na(x[eps_dates]))
      if(length(na_eps)==0){break()}
      if(fromNext){ # stops last entry trying to be filled from nonexistent next value
        if(eps_dates[na_eps[length(na_eps)]]==index(x)[nrow(x)]){
          na_eps <- na_eps[-length(na_eps)]
        }
      }
      na_eps_prev <- ind[eps[(na_eps)]-k]
      x[eps_dates[na_eps]] <- x[na_eps_prev]
    }
    df[,i] <- x
  }
  return(df)
}

#' Function to generate index of all NYSE trading dates.
nyse_trading_dates <- function(start_date, end_date, freq="days", tz="UTC"){
  start_y <- lubridate::year(start_date)
  end_y <- lubridate::year(end_date)
  master_ind <- seq(as.Date(start_date), as.Date(end_date), by=freq)
  master_ind <- master_ind[
    !((weekdays(master_ind) == "Saturday") |
        (weekdays(master_ind) == "Sunday"))]
  master_ind <- master_ind[!(master_ind %in% as.Date(
    timeDate::holidayNYSE(start_y:end_y)))]
  if (is.null(tz)) {tz=Sys.getenv("TZ")}
  master_ind <- as.POSIXct(master_ind, tz=tz)
  return(master_ind)
}

#' Function to generate index of all trading dates at a location supported
#' by the timeDate package.
trading_dates <- function(start_date, end_date, freq="days", tz="UTC",
                          loc="NYSE"){
  loc <- tolower(loc)[1]
  if(length(intersect(loc,c("nyse","asx","london","tsx","zurich","nerc"))) < 1){
    stop('loc must be one of: "NYSE","London","TSX","Zurich","NERC"')
  }
  f <- function(x){
    eval(parse(text=paste0("timeDate::holiday",toupper(loc),"()")))
  }
  start_y <- lubridate::year(start_date)
  end_y <- lubridate::year(end_date)
  master_ind <- seq(as.Date(start_date), as.Date(end_date), by=freq)
  master_ind <- master_ind[
    !((weekdays(master_ind) == "Saturday") |
        (weekdays(master_ind) == "Sunday"))]
  master_ind <- master_ind[!(master_ind %in% as.Date(f(start_y:end_y)))]
  if (is.null(tz)) {tz=Sys.getenv("TZ")}
  master_ind <- as.POSIXct(master_ind, tz=tz)
  return(master_ind)
}

#' NOT FOR EXPORT. Idiosyncratic function to tidy index membership xts.
#' @description Idiosyncratic function to clean symbol names of index
#' membership xts and replace names which had changed between data gathering
#'  and index membership update. Only created to maintain reproduceability.
#' @param hist_members xts object containing membership data. Column names are
#' assumed to be asset names corresponding to those in df. Note: All entries
#' not NA are assumed to represent active membership.
clean_hist_members <- function(hist_members,hist_index="SP500"){
  member_names <- gsub("/","_",names(hist_members))
  member_names <- gsub("\ .*","",member_names)
  names(hist_members) <- member_names
  if(hist_index=="SP500"){
    names(hist_members)[which(names(hist_members)=='CBRE')] <- "CBG"
    names(hist_members)[which(names(hist_members)=='WELL')] <- "HCN"
    names(hist_members)[which(names(hist_members)=='BKNG')] <- "PCLN"
    hist_members <- hist_members[,-(which(member_names=="1437355D"))] # Empty data
  }
  if(hist_index=="ASX200"){
    hist_members <- hist_members[,-(which(member_names=="IGR"))]
  }
  return(hist_members)
}

#' Function for extracing a given column from loaded xts objects and combining
#' them into a single object.
get_cols <- function(symbolList, cname){
  # Function to combine specified column (e.g. Close) of different object in
  # one data.frame or xts object
  df <- list(length(symbolList))
  for (i in 1:length(symbolList)) {
    x <- get(symbolList[i])
    x <- x[,cname]
    colnames(x) <- symbolList[i]
    df[[i]] <- x
  }
  df <- do.call(cbind,df)
  return(df)
}

#' Function returns logical if holding time exceeds the simulation end date.
holdingTime <- function(current_date,end_date,holding_time,holding_unit="month"){
  as.Date(ceiling_date(current_date %m+% months(holding_time),
                       unit=holding_unit)-days(1)) <= end_date
}

#' Function to perform commands in parallel
#' @importFrom foreach foreach
#' @importFrom foreach %dopar%
#' @examples
#' DF_cl <- get_cols(sp500,"Close")
#' ptext <- "naFillFrom(df=DF_cl[,i_1:i_2], ind=master_ind)"
#' DF_cl <- run_parallel(n=ncol(DF_cl), ptext=ptext)
#' @note Should wrap in tryCatch to close out cluster even if error
run_parallel_deprecated <- function(n, ptext, chunk_size=NULL){
  cores <- parallel::detectCores()
  if (is.null(chunk_size)){
    chunk_size=trunc(n/(detectCores()-1))
  } else if (chunk_size >= n){
    chunk_size=trunc(n/(detectCores()-1))
  }
  nums1 <- seq(1,n,chunk_size)
  nums2 <- nums1-1
  nums2 <- c(nums2[-1],n)
  mycluster <- parallel::makeCluster(cores-1,type = "FORK")
  doParallel::registerDoParallel(mycluster)
  # Begin iteration for chunk z
  out <- foreach(z = 1:length(nums1)) %dopar% {
    i_1 <- nums1[z]
    i_2 <- nums2[z]
    eval(parse(text=ptext))
  }
  parallel::stopCluster(mycluster)
  return(out)
}

#' Function to perform commands in parallel
#' @importFrom foreach foreach
#' @importFrom foreach %dopar%
#' @examples
#' DF_cl <- get_cols(sp500,"Close")
#' ptext <- "na.locf(df=DF_cl, ind=master_ind)"
#' DF_cl <- run_parallel(ptext=ptext,m=2)
#' @note Should wrap in tryCatch to close out cluster even if error
run_parallel_ <- function(ptext,m=2,chunk_size=NULL){
  # Attempt to obtain list of objects from string
  s1 <- get_objs(ptext)
  for(o in s1){
    k <- tryCatch(class(get(o)),error=function(e){NA})
    if(sum(c('xts','zoo','data.frame','tbl_df','tbl','matrix')==k)>0){
      ptext <- switch(m,'1'=sub(o,paste0(o,"[i_1:i_2,]"),ptext),
                      '2'=sub(o,paste0(o,"[,i_1:i_2]"),ptext))
      n_ops <- switch(m,'1'=nrow(get(o)),'2'=ncol(get(o)))
    }
    break()
  }
  # Setup cores and parallelisation parameters
  cores <- parallel::detectCores()
  if (is.null(chunk_size)){
    chunk_size=trunc(n_ops/(detectCores()-1))
  } else if (chunk_size >= n_ops){
    chunk_size=trunc(n_ops/(detectCores()-1))
  }
  nums1 <- seq(1,n_ops,chunk_size)
  nums2 <- nums1-1
  nums2 <- c(nums2[-1],n_ops)
  # Register clusters
  mycluster <- parallel::makeCluster(cores-1,type = "FORK")
  doParallel::registerDoParallel(mycluster)
  # Begin iteration for chunk z
  out <- foreach(z = 1:length(nums1)) %dopar% {
    i_1 <- nums1[z]
    i_2 <- nums2[z]
    tryCatch(eval(parse(text=ptext)), error=function(e){NA})
  }
  parallel::stopCluster(mycluster)
  # Combine result either by row or column
  f <- switch(m,'1'='rbind','2'=cbind)
  return(do.call(f,out))
}

#' Function to extract and neatly present the transactions of a list of
#' portfolios loaded into the environment.
txn_pretty <- function(portfolio_list){
  txn_master_list <- list()
  txn_list <- list()
  for (k in 1:length(portfolio_list)){
    assets <- names(blotter::getPortfolio(portfolio_list[k])$symbols)
    for (i in 1:length(assets)){
      sym <- assets[i]
      txns <- blotter::getTxns(portfolio_list[k], sym)
      txns$Symbol <- i
      txn_list[[i]] <- txns
    }
    txn <- do.call(rbind,txn_list)
    tm <- as.Date(index(txn))
    txns_df <- as.data.frame(txn,row.names = FALSE)
    txns_df$Date <- tm
    txns_df$Portfolio <- portfolio_list[k]
    txns_df <- txns_df[,
                       c("Date", "Portfolio",
                         "Symbol", setdiff(names(txns_df),
                                           c("Date", "Portfolio", "Symbol")))]
    txns_df$Symbol <- assets[txns_df$Symbol]
    txns_df <- txns_df[txns_df$Txn.Qty != 0, ]
    txn_master_list[[k]] <- txns_df
  }
  txn_master_list <- do.call(rbind,txn_master_list)
  return(dplyr::arrange(txn_master_list, Date, Portfolio, Symbol))
}

#' Dunction to creates news variables over n-month horizons. News items
#' released after COB are counted in next period.
#' @param master_ind Date or POSIXct array that will be used to determine
#' xta endpoints. Should reflect trading days.
#' @param eod_h Local time (24 hr format) of exchange close-of-business (int).
#' @param d_n xts of TRNA data (single entity).
#' @param eod_tz Time zone in which eod_h is specified (string).
#' @param n Formation period (months).
#' @param on Desired return periodicity (string). Passed to xts::endpoints.
#' @note Not for export!
monthlyNews_EOD <- function(master_ind,d_n,eod_h=16,eod_tz="EST",n=1,
                            on="months"){
  p_ind <- as.Date(master_ind)
  hour(p_ind) <- eod_h
  p_ind <- force_tz(p_ind,tz=eod_tz) #change to eod_tz, keeping eod_h clocktime
  #convert eod_h in eod_tz to associated hour in UTC
  p_ind <- with_tz(p_ind,tzone = "UTC")
  ep <- endpoints(p_ind, on=on) #first entry is always 0
  ep[1] <- 1
  d_n <- cbind(d_n,p_ind)
  nmat <- data.frame(matrix(nrow=length(ep),ncol = 13))
  nmat <- as.xts(nmat,order.by = p_ind[ep])
  for (i in 1:(length(ep)-n)){
    ind1 <- which(index(d_n)==p_ind[ep[i]]) #EOD last day of last month
    ind2 <- which(index(d_n)==p_ind[ep[i+n]]) #EOD last day of this month
    #EOD last day of last month to EOD last this of month
    news_subset <- d_n[ind1:ind2]
    #full_month <- fullmonth(d1=as.Date(p_ind[ep[i]]),d2=as.Date(p_ind[ep[i+1]]))
    days <- as.numeric(difftime(as.Date(p_ind[ep[i+n]]), as.Date(p_ind[ep[i]]),"days"))
    rel <- news_subset[,"relevance"] >= 0.6
    newsDays <- length(unique(as.Date(index(news_subset[rel,]))))
    stories <- news_subset[,"urgency"]==3
    alerts <- news_subset[,"urgency"]==1
    rel_summation <- sum(news_subset[rel,"relevance"],na.rm = TRUE)
    rel_summation_stories <- sum(news_subset[rel&stories,"relevance"],na.rm = TRUE)
    rel_summation_alerts <- sum(news_subset[rel&alerts,"relevance"],na.rm = TRUE)
    raw_stories <- sum(stories,na.rm = TRUE) # no raw stories
    raw_alerts <- sum(alerts,na.rm = TRUE) # no raw alerts
    rel_stories <- sum(rel&stories,na.rm = TRUE) # no relevant stories
    rel_alerts <- sum(rel&alerts,na.rm = TRUE) # no rel alerts
    NoPos <- sum(news_subset[rel,"sentimentClass"]==1) # no + relevant items
    NoNeg <- sum(news_subset[rel,"sentimentClass"]==-1) # no - relevant items
    # rel-wgted story sent
    story_relwgted_sent <- sum(
      news_subset[rel&stories,"relevance"]*
        (news_subset[rel&stories,"sentimentPositive"]-
           news_subset[rel&stories,"sentimentNegative"]),
      na.rm = TRUE)/rel_summation_stories
    # rel-wgted alert sent
    alert_relwgted_sent <- sum(news_subset[rel&alerts,"relevance"]*
                                 (news_subset[rel&alerts,"sentimentPositive"]-
                                    news_subset[rel&alerts,"sentimentNegative"])
                               ,na.rm = TRUE)/rel_summation_alerts
    # sd of sent
    sd_sent <- sd((news_subset[rel,"sentimentPositive"]-
                     news_subset[rel,"sentimentNegative"]),na.rm = TRUE)
    # rel-wgted sent all
    relwgted_sent <- sum(
      (news_subset[rel,"sentimentPositive"]-
         news_subset[rel,"sentimentNegative"])*
        news_subset[rel,"relevance"],na.rm=TRUE)/rel_summation
    # sent all
    sent <- mean(
      news_subset[rel,"sentimentPositive"]-
        news_subset[rel,"sentimentNegative"],na.rm=TRUE)
    nmat[i+n,] <- c(raw_stories,raw_alerts,rel_stories,rel_alerts,NoPos,
                    NoNeg,story_relwgted_sent,alert_relwgted_sent,sd_sent,
                    relwgted_sent,sent,days,newsDays)
  }
  names(nmat) <- c("raw_stories","raw_alerts","rel_stories","rel_alerts",
                   "relPos","relNeg","story_relwgted_sent","alert_relwgted_sent",
                   "sd_sent","relwgted_sent","sent","days","news_days")
  #index(nmat) <- as.Date(index(nmat))
  #nmat$Member <- as.numeric(d_p[index(nmat),'Member'])
  #nmat$Member <- as.numeric(d_p[as.Date(index(nmat)),'Member'])
  return(nmat)
}


#' Construct end-of-month measures of firm data.
#' @note Not for export!
MonthlyFirmData <- function(f_d, d_p, ind=NULL){
  if (is.null(ind)){
    ind <- seq(as.Date(index(f_d)[1]), as.Date(index(f_d)[nrow(f_d)]), by="days")
    ind <- ind[!((weekdays(ind) == "Saturday") | (weekdays(ind) == "Sunday"))]
    ind <- ind[!(ind %in% as.Date(holidayNYSE(
      year(index(f_d))[1]:year(index(f_d))[nrow(f_d)])))]
  }
  f_d <- cbind(f_d,ind) ; f_d <- cbind(f_d,d_p[,c("PX_LAST","Member")])
  f_d[is.na(f_d$Member),"Member"] <- 0
  f_d <- f_d[ind]
  f_d <- fillFwd(f_d,"HISTORICAL_MARKET_CAP")
  f_d[,"PX_LAST"] <- naFillFrom(f_d[,"PX_LAST"])
  f_d$Mkt_cap_calc <- (1/f_d[,"PX_TO_BOOK_RATIO"])*
    (f_d[,"MARKET_CAPITALIZATION_TO_BV"])*(f_d[,"PX_LAST"])
  # Get end-of-months
  eps <- endpoints(index(f_d), on="months")
  eps[1] <- 1
  # Empty matrix for holding values
  fmat <- data.frame(matrix(nrow=length(eps)-1,ncol = 8))
  fmat <- as.xts(fmat,order.by = index(f_d)[eps[-1]])
  names(fmat) <- c("Mkt_cap","BookToMkt","Analyst","PxToBook","Turnover",
                   "Vol20d", "Mkt_cap_calc","Member")
  for (i in 1:(length(eps)-1)){
    firm_subset <- f_d[(eps[i]+1):eps[i+1]]
    if(sum(!is.na(firm_subset$Mkt_cap_calc))<=10){next()}
    #firm_subset <- firm_subset[which(!is.na(firm_subset$TURNOVER))]
    mkt <- firm_subset[nrow(firm_subset),"HISTORICAL_MARKET_CAP"]
    fmat[i,1] <- mkt
    MB <- mean(firm_subset[,"MARKET_CAPITALIZATION_TO_BV"],na.rm=TRUE)
    fmat[i,2] <- 1/MB
    A <- mean(firm_subset[,"TOT_ANALYST_REC"],na.rm=TRUE)
    fmat[i,3] <- A
    PB <- mean(firm_subset[,"PX_TO_BOOK_RATIO"],na.rm=TRUE)
    fmat[i,4] <- PB
    TO <- mean(firm_subset[,"TURNOVER"],na.rm=TRUE)
    fmat[i,5] <- TO
    if (sum(is.na(firm_subset$VOLATILITY_20D))==nrow(firm_subset)){
      V <- NA
    } else{
      V <- which(!(is.na(firm_subset[,"VOLATILITY_20D"])))
      V <- firm_subset[V[length(V)],"VOLATILITY_20D"] #most recent 20day vol
    }
    fmat[i,6] <- V
    fmat[i,7] <- mean(firm_subset[,"Mkt_cap_calc"],na.rm = TRUE)
    fmat[i,8] <- mean(firm_subset[,"Member"],na.rm = TRUE)
  }
  fmat <- fmat[!is.na(fmat[,"Turnover"]),]
  return(fmat)
}


#' Function to extract news variables for a list of assets over a specified
#' formation period and save in individual .csv files.
#' @note Not for export.
get_news_vars <- function(newsDir, saveDir, assets, n=1, ind, eod_h=16,
                          eod_tz="EST", on="months"){
  n_list <- gsub(".csv","",list.files(newsDir))
  NoData <- array()
  for (z in 1:length(sp500)){
    stock <- assets[z]
    if (!(stock %in% n_list)){
      print(paste("DATA NOT FOUND FOR TICKER",stock))
      NoData <- c(NoData,stock)
    } else {
      print(paste("z = ",z," stock = ",stock))
      sname <- paste0(stock,".csv")
      d_n <- read_csv(file.path(trnaDir,sname))
      #L <- get_Lct(d_n)
      #d_n <- cbind(d_n[,-which(names(d_n)=="noveltyCountValues")],L)
      d_n <- d_n[,c("feedTimestamp","relevance","urgency","sentimentClass",
                    "sentimentPositive","sentimentNegative")]
      d_n <- as.xts(d_n[,-which(names(d_n)=="feedTimestamp")],
                    order.by = d_n$feedTimestamp)
      nmat <- monthlyNews_EOD(ind,d_n=d_n,eod_h=eod_h,eod_tz=eod_tz,n=n,on=on)
      if(class(nmat)[1] != "xts"){
        NoData <- c(NoData,stock)
        next()
      }
      ind_n <- index(nmat)
      df <- as.data.frame(nmat)
      df$Date <- ind_n
      if (!dir.exists(saveDir)){ dir.create(saveDir) }
      write_csv(df,file.path(saveDir,sname))
    }
  }
  return(NoData)
}

#' Function to extract firm variables for a list of assets over a specified
#' formation period and save in individual .csv files.
#' @note Not for export.
get_firm_data <- function(firm_saveDir, firmDir, priceDir, assets, ind=NULL){
  p_list <- gsub(".csv","",list.files(priceDir))
  f_list <- gsub(".csv","",list.files(firmDir))
  NoData <- array()
  for (z in 1:length(sp500)){
    stock <- assets[z]
    if (( !(stock %in% p_list) | !(stock %in% f_list)) ){
      print(paste("DATA NOT FOUND FOR TICKER",stock))
      NoData <- c(NoData,stock)
    } else {
      print(paste("z = ",z," stock = ",stock))
      sname <- paste0(stock,".csv")
      d_p <- readxts2(dir=priceDir,fname=sname)
      f_d <- readxts2(firmDir,sname,index.col = "date")
      if(dim(f_d)[1] <= 10 | dim(d_p)[1] <= 10){
        NoData <- c(NoData,stock)
        print(paste("DATA EMPTY FOR TICKER",stock))
        next()}
      fmat <- MonthlyFirmData(f_d, d_p, ind=ind)
      tm <- index(fmat)
      df <- as.data.frame(fmat)
      df$date <- tm
      if (!dir.exists(firm_saveDir)){ dir.create(firm_saveDir) }
      write_csv(df,file.path(firm_saveDir, sname))
    }
  }
  return(NoData)
}

#' Function to calculate portfolio equity given weights. Unlike the version
#' from PerformanceAnalytics, can handle long-short portfolios. Returns equity
#' curve as opposed to returns.
portfolio_cumulative.return <- function(df, weights=NULL, rebalance_on=NULL,
                                        verbose=FALSE, zero_cut=TRUE){
  if(!("xts" %in% class(df))|!("zoo" %in% class(df))){
    stop("df must be an xts or zoo object.")
  }
  if(class(weights)[1]!="numeric" & !(is.null(weights))){
    if(!("xts" %in% class(weights))|!("zoo" %in% class(weights))){
      stop("Weights must be an xts, zoo or numeric object.")
    }
    if(ncol(df)!=ncol(weights)){
      stop("Number of columns in weights must be equal
           to number of columns in df.")
    }
    } else {
      if(is.null(weights)){
        weights <- rep(1,ncol(df))/ncol(df)
        warning("Weights not specified. Assuming equal-weighted portfolio.")
      }
      if(length(weights)!=ncol(df)){
        stop("Number of weights must be equal
             to number of columns in df.")
      }
      if(!is.null(rebalance_on)){
        if(rebalance_on=="days"){
          rets <- na.fill(df,0)
          for(k in 1:ncol(df)){
            rets[,k] <- rets[,k]*weights[k]
          }
          eq <- xts(cumprod(1+rowSums(rets)),order.by=index(rets))
          return(eq)
        } else {
          eps <- xts::endpoints(df,on=rebalance_on); eps[1] <- 1
        }
      } else {eps <- 1}
      weights <- xts(matrix(rep(weights,length(eps)),ncol=ncol(df),
                            byrow=TRUE), order.by = index(df)[eps])
      }
  df <- zoo::na.fill(df,0)
  df <- df[paste0(as.Date(zoo::index(weights)[1]),"/")]
  # Weight vector with only positive weights
  w_pos <- as.numeric(as.numeric(weights>0))*weights
  # initialise return contribution matrix
  c_mat <- xts(matrix(0,nrow=nrow(df),ncol = ncol(df)),order.by = index(df))
  c_mat[index(weights)[1],] <- w_pos[1,]
  bop <- 1 #initialise beginning of period wealth
  for (i in 1:nrow(weights)){
    # If equity is negative and zero_cut==TRUE, don't enter new positions
    if(bop <= 0){
      if(zero_cut){
      #weights[i,] <- 0
      from = as.Date(index(weights[i,]))+1
      to = as.Date(last(index(df)))
      drange <- paste0(from, "::", to)
      eq <- xts(rowSums(c_mat),order.by = index(c_mat))
      eq[drange] <- bop
      if(verbose){
        return(list(eq,c_mat))
      } else {return(eq)}
      }
      }
    from = as.Date(index(weights[i,]))+1
    if (i == nrow(weights)){
      to = as.Date(last(index(df)))
    } else {
      to = as.Date(index(weights[(i+1),]))
    }
    drange <- paste0(from, "::", to)
    returns = df[drange]
    cum_rets <- (cumprod(1+returns))
    for (j in which(weights[i,] != 0)){
      c_mat[drange,j] <- ((cum_rets[,j]-1)*as.numeric(weights[i,j])*abs(bop)
                          + as.numeric(w_pos[i,j])*bop)
    }
    bop <- sum(c_mat[to])
  }
  eq <- xts(rowSums(c_mat),order.by = index(c_mat))
  if(verbose){
    return(list(eq,c_mat))
  } else {return(eq)}
}

#' Calculate simple returns.
simpleRets <- function(prices){
  prices/xts::lag.xts(prices,k=1)-1
}

#' Function to attribute news items to the correct trading day
allocate_news <- function(d_n, nyse=TRUE, eod_h=16, eod_tz="EST", ind=NULL){
  # get date times
  ind1 <- index(d_n)
  # convert date times to exchange local time
  ind1 <- lubridate::with_tz(ind1, tzone=eod_tz)
  # get date version of ind1 (required as can't mix dates and posixct in index)
  ind2 <- as.Date(ind1, tz=eod_tz)
  # News released after cob is attributed to next day
  ind2[hour(ind1) >= eod_h] <- ind2[hour(ind1) >= eod_h] + days(1)
  if(nyse){
    master_ind <- as.Date(nyse_trading_dates(ind1[1],
                                             ind1[length(ind1)]+days(5)))
  } else {
    if(is.null(ind)){
      stop("Must provide own index of business days unless assuming NYSE")
    }
    master_ind <- ind
  }
  # News occuring on non-business days is attributed to next business day
  nb <- which(!(ind2 %in% master_ind))
  for ( i in 1:length(nb)){
    ind2[nb[i]] <- master_ind[(master_ind > ind2[nb[i]])][1]
  }
  index(d_n) <- ind2
  return(d_n)
}

#' Function to calculate daily news scores from xts of news data.
#' Applied to output of `allocate_news`.
#' "by_story" aggregates to story level first
daily_news_scores <- function(dn, min_relevance=0.6, news_type="all", ind=NULL){
  news_type <- tolower(news_type)
  if(!(news_type %in% c("all", "combined", "takes", "alerts","by_story"))){
    warning(paste('news_type must be one of "all", "combined", "takes",',
                  '"by_story", or "alerts". Assuming "all".'))
    news_type <- "all"
  }
  if(is.null(ind)){ind <- unique(index(dn))}
  tm <- index(dn)
  dn <- as.data.frame(dn, stringsAsFactors=FALSE)
  dn <- utils::type.convert(dn)
  dn$date <- tm
  row.names(dn)<- NULL
  dn <- subset(dn, relevance >= min_relevance)
  if(nrow(dn)==0){
    stop("No observations satisfy relevance threshold.")
  }

  if(news_type =="by_story"){
    dn <- dplyr::group_by(dn, date, altId)
    smt <- dplyr::summarise(dn,
                            t_sent=sum(relevance*(sentimentPositive-sentimentNegative))
                            /sum(relevance),story_relevance=mean(relevance),
                            n_alerts=sum(urgency==1),n_takes=sum(urgency==3))
    # Aggregate by day
    smt <- dplyr::group_by(smt, date)
    smt <- dplyr::summarise(smt,t_sent=
                              sum(story_relevance*t_sent)/sum(story_relevance),
                            alerts=sum(n_alerts),takes=sum(n_takes),
                            stories=n())

    smt <- xts(smt[,setdiff(names(smt),"date")],order.by = as.Date(smt$date))
    smt <- cbind(smt,ind)
    smt$stories[is.na(smt$stories)] <- 0
    smt$alerts[is.na(smt$alerts)] <- 0
    smt$takes[is.na(smt$takes)] <- 0
    return(smt)
  }
  dn <- dplyr::group_by(dn, date)
  if(news_type =="all"){
    sms <- dplyr::summarise(subset(dn, urgency==3), takes=n(),
                            s_sent=sum(relevance*(sentimentPositive-sentimentNegative))
                            /sum(relevance))
    sma <- dplyr::summarise(subset(dn, urgency==1), alerts=n(),
                            a_sent=sum(relevance*(sentimentPositive-sentimentNegative))
                            /sum(relevance))
    smt <- dplyr::summarise(dn,
                            t_sent=sum(relevance*(sentimentPositive-sentimentNegative))
                            /sum(relevance))
    sms <- xts(sms[,setdiff(names(sms),"date")],order.by = as.Date(sms$date))
    sma <- xts(sma[,setdiff(names(sma),"date")],order.by = as.Date(sma$date))
    smt <- xts(smt[,setdiff(names(smt),"date")],order.by = as.Date(smt$date))
    dc <- cbind(cbind(smt,sms,sma),ind)
    dc$takes[is.na(dc$takes)] <- 0
    dc$alerts[is.na(dc$alerts)] <- 0
    return(dc)
  }
  if(news_type =="combined"){
    smt <- dplyr::summarise(dn,
                            t_sent=sum(relevance*(sentimentPositive-sentimentNegative))
                            /sum(relevance),items=n())
    smt <- xts(smt[,setdiff(names(smt),"date")],order.by = as.Date(smt$date))
    smt <- cbind(smt,ind)
    smt$items[is.na(smt$n)] <- 0
    return(smt)
  }
  if(news_type =="takes"){
    sms <- dplyr::summarise(subset(dn, urgency==3), takes=n(),
                            s_sent=sum(relevance*(sentimentPositive-sentimentNegative))
                            /sum(relevance))
    sms <- xts(sms[,setdiff(names(sms),"date")],order.by = as.Date(sms$date))
    sms <- cbind(sms,ind)
    sms$takes[is.na(sms$takes)] <- 0
    return(sms)
  }
  if(news_type =="alerts"){
    sma <- dplyr::summarise(subset(dn, urgency==1), alerts=n(),
                            a_sent=sum(relevance*(sentimentPositive-sentimentNegative))
                            /sum(relevance))
    sma <- xts(sma[,setdiff(names(sma),"date")],order.by = as.Date(sma$date))
    sma <- cbind(sma,ind)
    sma$alerts[is.na(sma$alerts)] <- 0
    return(sma)
  }
}

#' Make to handle combined news xts?? That way can use centered vars
#' Currently weights each sentiment day for each asset by relative recency
#' of the sentiment day with respect to the assets own news history *not*
#' relative to absolute recency.
#' @param  dn combined xts of daily news measure for multiple assets. e.g.
#' get_cols applied to output of `daily_news_scores`
aggregate_news <- function(dn, n=1, w=1, on="months", ind=NULL, from_first=TRUE){
  if(!is.null(ind)){dn <- cbind(dn,ind); dn <- dn[ind]}
  eps <- xts::endpoints(dn, on=on)
  if (from_first) {eps[1] <- 1} else { eps <- eps[-1]}
  nmat <- data.frame(matrix(nrow=length(eps),ncol = ncol(dn)))
  nmat <- as.xts(nmat,order.by = index(dn)[eps])
  colnames(nmat) <- colnames(dn)
  eps[1] <- eps[1]-1
  if(w == 1){ # average
    for (i in 1:(length(eps)-n)){
      news_subset <- dn[(eps[i]+1):eps[i+n],]
      for (j in 1:ncol(dn)){
        ns <- news_subset[,j]
        if(sum(is.na(ns))==nrow(ns)){next()}
        val <- mean(ns,na.rm=TRUE)
        nmat[index(dn)[eps[i+n]],j] <- val
      }
    }
  } else{
    for (i in 1:(length(eps)-n)){
      news_subset <- dn[(eps[i]+1):eps[i+n],]
      for (j in 1:ncol(dn)){
        ns <- news_subset[,j]
        if(sum(is.na(ns))==nrow(ns)){next()}
        wt <- seq(1,w,length.out = nrow(ns))/w
        ns$wt <- wt-max(0, wt[(which(!is.na(ns))[1]-1)])# Translation invariance
        ns <- na.omit(ns)
        val <- sum(ns[,1]*ns$wt)/sum(ns$wt)
        nmat[index(dn)[eps[i+n]],j] <- val
      }
    }
  }
  return(nmat)
}


#' Temporary function to load and return news data and convert to xts
#' (does not put in environment)
load_news_file <- function(f_dir, f_name, col_names = NULL){
  if(is.null(col_names)){
    col_names <- c("feedTimestamp","relevance","urgency","sentimentClass",
                   "sentimentPositive","sentimentNegative","altId")
  }
  x <- read_csv(file.path(f_dir,paste0(f_name,".csv")))
  x <- x[,col_names]
  x <- as.xts(x[,-which(names(x)=="feedTimestamp")],
              order.by = x$feedTimestamp)
}

#' Function to calculate number of news days or number of news items over
#' formation period.
#' @param  dn combined xts of daily news measure for multiple assets. e.g.
#' get_cols applied to output of `daily_news_scores`
news_days <- function(dn, n=1, on="months", ind=NULL, from_first=TRUE,
                      items=FALSE){
  if(!is.null(ind)){dn <- cbind(dn,ind); dn <- dn[ind]}
  eps <- xts::endpoints(dn, on=on)
  if (from_first){eps[1] <- 1} else { eps <- eps[-1]}
  nmat <- as.data.frame(matrix(0,ncol=ncol(dn),nrow=length(eps)))
  nmat <- as.xts(nmat,order.by = index(dn)[eps])
  colnames(nmat) <- colnames(dn)
  eps[1] <- eps[1]-1
  if(items==FALSE){
    for (i in 1:(length(eps)-n)){
      news_subset <- dn[(eps[i]+1):eps[i+n],]
      for (j in 1:ncol(dn)){
        nmat[index(dn)[eps[i+n]],j] <- sum(!is.na(news_subset[,j]))
      }
    }
  } else{
    for (i in 1:(length(eps)-n)){
      news_subset <- dn[(eps[i]+1):eps[i+n],]
      for (j in 1:ncol(dn)){
        nmat[index(dn)[eps[i+n]],j] <- sum(news_subset[,j],na.rm = TRUE)
      }
    }
  }
  nmat[1:n,] <- NA
  return(nmat)
}

#' Function to calculate forward return
forward_return <- function(df,on="months",n=6,ind=NULL,backward=FALSE){
  nms <- names(df)
  if (is.null(ind)){
    ind <- nyse_trading_dates(as.Date(index(df)[1]),
                              as.Date(index(df)[nrow(df)]))
  }
  # merge with ind to ensure swapping indices later on is consistent.
  df <- cbind(df,ind)
  # Periodicity of return
  if (on=="weeks") {
    f = xts::to.weekly
  } else {f = xts::to.monthly}
  # Use the last available price for each period.
  # Specifying "lastof" allows us to cbind results,
  # since the last day of each period (rather than that of last data) is used.
  df  <- lapply(1:ncol(df),FUN=function(x){
    f(na.omit(df[,x]),indexAt="lastof",OHLC=FALSE)})
  df <- do.call(cbind,df)
  eps <- endpoints(ind,on=on,k=1)[-1]
  # Change from last day of each period to last trading day
  index(df) <- ind[eps]
  # Calculate n-period return
  df <- ROC(df,n=n,type="discrete")
  if(backward) {names(df) <- nms; return(df)}

  df[1:(nrow(df)-n),] <- as.matrix(df[(n+1):nrow(df),])
  df[(nrow(df)-n+1):nrow(df),] <- NA

  names(df) <- nms
  return(df)
}

#' Function to median-centre aggregated news sentiment and
#' zero-fill no-news stocks (missing values) for index members.
median_adjust_deprecated <- function(ns,hist_members=newHM){
  #ns is temporally aggregated (not daily) sentiment scores
  #med_sent <- xts(rep(0,nrow(ns)),order.by = index(ns))
  #names(med_sent) <- c("median_sent")
  ns_adj <- ns
  for (i in 1:nrow(ns_adj)){
    idate <- as.Date(index(ns)[i])
    if(sum(is.na(ns[idate,]))==ncol(ns)){next()}
    members <- names(hist_members)[which(!is.na(hist_members[idate,]))]
    members_with_data <- members[!is.na(as.numeric(ns[idate,members]))]
    if(length(members_with_data)==0){next()}
    ms <- median(ns[idate,members_with_data],na.rm = TRUE)
    #med_sent[idate,1] <- ms
    ns_adj[idate,members_with_data] <- as.numeric(ns[idate,members_with_data])-ms
    ns_adj[idate,is.na(ns_adj[idate,])] <- 0
  }
  return(ns_adj)
}

#' Function to median-centre aggregated news sentiment and
#' zero-fill no-news stocks (missing values) for index members.
#' Note: this function relies on hist_members containing NAs
#' to represent non-membership (as output by Bloomberg wrappers)
#' Note also that news data for all non-members will be zero.
median_adjust <- function(ns,hist_members=newHM,func=median,zero_fill=TRUE){
  # need to recall which rows have no data
  no_data <- apply(ns,1,function(x) sum(is.na(x))) == ncol(ns)
  hist_members <- hist_members[index(ns),names(ns)]
  ns <- ns*hist_members # This makes non-member news data NA
  ns <- ns-apply(ns,1,func,na.rm=TRUE)
  if(zero_fill){
    # Fill all NAs with zeros, but make non-members NA again, since they were not
    # median-adjusted and their zero-values will be meaningless.
    ns <- zoo::na.fill(ns,0)*hist_members
    # also make rows with no data NA again
  }
  ns[no_data,] <- NA
  return(ns)
}

fillFwd <- function(df,colname){
  pts <- which(!is.na(df[,colname]))
  if (length(pts) > 1){
    for (i in 1:(length(pts)-1)){
      if ((pts[i]+1)==pts[i+1]){next()}
      df[(pts[i]+1):(pts[i+1]-1),colname] <- df[pts[i],colname]
    }
  }
  if (length(pts) >= 1){
    if(pts[length(pts)] != nrow(df)){
      df[(pts[length(pts)]+1):nrow(df),colname] <- df[pts[length(pts)],colname]
    }
  }
  return(df)
}

#' New much, much faster function to calculated cumulative long-short portfolio
#' returns.
rp_cum_rets <-  function(wts,df_rets,stop_loss=NULL){
  bop=1
  eq <- xts(matrix(nrow=nrow(df_rets),ncol = 1),order.by = index(df_rets))
  for (i in 1:nrow(wts)){
    if( sum(abs(wts[i,]),na.rm = TRUE) == 0 ){next()}
    sdate=as.Date(index(wts)[i])+1
    if(sdate >= last(index(df_rets))){next()}
    if(i == nrow(wts)){
      edate = as.Date(last(index(df_rets)))
    } else { edate=as.Date(index(wts)[i+1]) }
    drange=paste0(sdate,"::",edate)
    # Short side
    short_syms <- names(wts)[which(wts[i,] < 0)]
    s_ret=0
    wts_sum_s=0
    if (length(short_syms)!=0){
      short_wts <- as.numeric(abs(wts[i,short_syms]))
      wts_sum_s <- sum(short_wts)
      s_ret <- apply(cumprod(1+df_rets[drange,short_syms]),1,
                     weighted.mean,w=short_wts)
      #s_ret <- xts(s_ret,order.by = index(df_rets[drange,]))
      s_ret <- -(s_ret*wts_sum_s)
    }
    l_ret=0
    long_syms <- names(wts)[which(wts[i,] > 0)]
    wts_sum_l=0
    if (length(long_syms)!=0){
      long_wts <- as.numeric(abs(wts[i,long_syms,]))
      wts_sum_l <- sum(long_wts)
      l_ret <- apply(cumprod(1+df_rets[drange,long_syms]),1,
                     weighted.mean,w=long_wts)
      #l_ret <- xts(l_ret,order.by = index(df_rets[drange,]))
      l_ret <- l_ret*wts_sum_l
    }
    lsm <- xts((l_ret+s_ret),order.by = index(df_rets[drange,]))
    #eq[drange] <- bop*(l_ret+s_ret)+bop
    if(!is.null(stop_loss)){
      if(min(lsm) <= -1*stop_loss){
        ws <- which(lsm <= -1*stop_loss)[1]
        lsm[ws:length(lsm)] <- lsm[ws]
      }
    }
    eq[drange] <- bop*(lsm+1-(wts_sum_l-wts_sum_s))
    bop <- as.numeric(last(eq[drange]))
    if (bop <= 0){
      ws <- which(eq <= 0)[1]
      eq[ws:nrow(eq)] <- 0
      return(eq)
    }
  }
  return(na.locf(eq))
}

#' Function for adjusting prices or dividends for splits.
#' splits should be recorded as dilution factor, e.g. a 2:1 split, where
#' the number of shares doubles, would be 0.5.
#' This is the form returned by quantmod::getSplits().
ca_adjust <- function(x, splits){
  x <- x[!duplicated(index(x)),]
  ind <- index(x)
  x <- na.omit(x)
  d <- cbind(x,splits)[paste0(range(index(x)),collapse = "::")]
  for (j in which(!is.na(d[,2]))){
    d[min((j-1),1):(j-1),1] <- d[min((j-1),1):(j-1),1]*as.numeric(d[j,2])
  }
  # return with original index and missing values
  adj_divs <- cbind(d[index(x),1],ind)
  return(adj_divs)
}

#' Function for adjusting prices for dividends
div_adjust <- function(x, divs){
  x <- x[!duplicated(index(x)),]
  # remove duplicate dividends (if you want summed, should be added elsewhere)
  is_duplicated <- duplicated(cbind(index(divs),as.numeric(divs)))
  divs <- divs[!is_duplicated,]
  ind <- index(x)
  x <- na.omit(x)
  d <- cbind(x,divs)[paste0(range(index(x)),collapse = "::")]
  # If close not available on ex date, use next close for adj ratio.
  d[,1] <- zoo::na.locf(d[,1], fromLast = TRUE)
  for (j in which(!is.na(d[,2]))){
    rj <- as.numeric(d[j,1]/(d[j,1]+d[j,2]))
    d[min((j-1),1):(j-1),1] <- d[min((j-1),1):(j-1),1]*rj
  }
  # return with original index and missing values
  adj_divs <- cbind(d[index(x),1],ind)
  return(adj_divs)
}

#' Function for adjusting prices for splits and dividends
#' cch are splits that need to be used to adjust dividends
#' if splits are provided by cch is null, all splits will be used to adjust
#' dividends.
#' Approach is: adjust dividends, adjust prices for splits, adjust
#' split-adjusted prices for dividends using adjusted dividends.
adjust_prices <- function(x, divs=NULL, splits=NULL, cch=NULL){
  if(!is.null(splits)){
    if(!is.null(divs)){
      if(is.null(cch)){cch <- splits}
      td_a <- ca_adjust(divs, splits = cch)
      tp_a <- ca_adjust(x, splits = splits)
      tp_a <- div_adjust(tp_a, divs = td_a)
    } else {
      tp_a <- ca_adjust(x, splits = splits)
    }
  } else {
    tp_a <- div_adjust(x, divs=divs)
  }
  return(tp_a)
}

#' Function for converting xts to data.frames with time index
#' for the purpose of saving to file.
xts_to_df <- function(x, datecol="date"){
  nm <- names(x)
  tm <- index(x)
  x <- data.frame(x)
  names(x) <- nm
  x$date <- tm
  x <- x[,c("date",nm)]
  row.names(x) <- NULL
  return(x)
}

#' Function to load and combine raw trna news and score files
load_raw_news_files <- function(Dir,n_file,s_file,scorenames, newsnames){
  df <- read_tsv(file.path(Dir,s_file))
  df <- df[,scorenames]
  dn <- read_tsv(file.path(Dir,n_file))
  dn <- dn[,newsnames]
  return(inner_join(df,dn,by='id'))
}

#' Gets columns of all symbols (e.g. close price) by loading each
#' file and discarding it, rather than needed all files loading in
#' Global environmnet.
get_cols_large <- function(assets,f_dir,cname,daterange=NULL){
  p_list <- list.files(f_dir)
  no_data <- assets[!(paste0(assets,".csv") %in% p_list)]
  col_names = c()
  if(length(no_data) >= 1){
    print(paste("No Data Found For ",no_data))
  }
  M <- list()
  for ( i in 1:length(assets)){
    sym <- assets[i]
    if(sym %in% no_data){next()}
    x <- tryCatch(
      readxts2(dir=f_dir,fname=paste0(sym,".csv"),daterange=daterange),
      error=function(e) NA
    )
    if(class(x)[1] != "xts"){
      print(paste("Data for ",sym," is empty!"))
      next()}
    x <- x[,cname]
    col_names <- c(col_names,sym)
    M[[i]] <- x
  }
  M <- do.call(cbind,M)
  names(M) <- col_names
  return(M)
}

#' Gets trading ASX days from xts of reference stocks by removing days with no
#' trading. I.e. if any stock has price data on a given day, that day is
#' assumed to be a trading day.
asx_trading_dates <- function(df_cl=NULL,file_path=processed_dir){
  if(is.null(df_cl)){
    trade_dates <- readRDS(file.path(processed_dir,
                                     "asx200_trade_dates.RDS"))
    return(trade_dates)
  }
  k <- apply(df_cl,1,function(x) sum(!is.na(x)))
  trade_dates <- index(df_cl)[k!=0]
  return(trade_dates)
}

#' Function for sorting rows of xts or data frame into groupings based on value.
#' Does not take into account membership. NAs remain as NAs.
#' @param df An xts, matrix, or data.frame object.
#' @param groupings Number of groups in each row.
#' @param low_to_high If TRUE, high values will be associated with a higher
#' @param ties_method `ties.method` parameter for base::rank()
#' group number.
quantile_sort <- function(df, groupings, low_to_high=TRUE,ties_method='first'){
  # Get rows that aren't all NAs
  k <- rowSums(is.na(df)) != ncol(df)
  # Matrix to store results
  x <- matrix(nrow=nrow(df),ncol = ncol(df))
  # A 1 will be in a numerically lower group than a 10
  if(low_to_high){
    labs <- 1:groupings
  } else{
    # A 10 will be in a numerically lower group than a 1
    labs <- groupings:1
  }
  for(i in which(k)){
    x[i,!is.na(df[i,])] <- as.numeric(
      cut_number(rank(na.omit(as.numeric(df[i,])),ties.method=ties_method),
          breaks=groupings, labels=labs))
  }
  if ('xts' %in% class(df)){
    x <- xts::xts(x,order.by = zoo::index(df))
  } else if ( "data.frame" %in% class(df)){
    x <- as.data.frame(x)
  }
  colnames(x) <- colnames(df)
  return(x)
}

#' Function to conduct sequential sorting with respect to a number of variables
#' and an index.
#' @param df An xts, matrix, or data.frame object.
#' @param vars A vector of variable names (in order) to sort by
#' @param groupings Number of groups for each variable.
#' @param index String indicating grouping variable (i.e. date)
#' @param low_to_high If TRUE, high values will be mapped to high group numbers
multi_sort_long <- function(df,vars,groupings,index,low_to_high=TRUE){
  # Function to catch errors when not enough non-missing values to create bins.
  cn <- function(x,y,z){
    tryCatch(ggplot2::cut_number(x,n=y,labels=z),
             error=function(e) rep(NA,length(x)))
  }
  ind <- index
  gs <- groupings
  labs <- if(low_to_high){gs[1]:1} else{1:gs[1]}
  df <- group_by(df,!!as.symbol(ind)) %>%
    mutate((!!as.symbol(paste0(vars[1],"_g"))) :=
             cn(!!as.symbol(vars[1]),gs[1],labs)) %>% ungroup()
  # Convert from factor to numeric
  df[paste0(vars[1],"_g")] <- df[[paste0(vars[1],"_g")]] %>% as.character() %>% 
    as.numeric()
  if(length(vars)>1){
    for(i in 2:length(vars)){
      labs <- if(low_to_high){gs[i]:1} else{1:gs[i]}
      df <- group_by(df, !!as.symbol(ind), !!as.symbol(paste0(vars[i-1],"_g"))) %>%
        mutate((!!as.symbol(paste0(vars[i],"_g"))) :=
                 cn(!!as.symbol(vars[i]),gs[i],labs)) %>% ungroup()
      # Convert from factor to numeric
      df[paste0(vars[i],"_g")] <- df[[paste0(vars[i],"_g")]] %>% 
        as.character() %>% as.numeric()
    }
  }
  return(df)
}

#' Function to quickly calculate linear regression coefficients.
ols <- function (y, x) {
  xy <- t(x)%*%y
  xxi <- solve(t(x)%*%x)
  b <- as.vector(xxi%*%xy)
  b
}

#' Function to calculate return convexity for wide xts of returns a la
#' Chen et al. (2018) "Evolution of historical prices in momentum investing".
accel <- function(df,j,on="months",dlim){
  eps <- endpoints(df,on=on,k=1)
  eps[1] <- 1
  res_list <- vector('list',ncol(df))
  for(k in 1:ncol(df)){
    res <- numeric(length(eps)-j)
    d <- df[,k]
    for(i in (j+1):length(eps)){
      # Get J period subset
      y <- d[eps[i-j]:eps[i],1]
      # Check recent trading
      cond1 <- sum(is.na(tail(y,10))) > 6
      # Check minimum amount of trading days
      y <- na.omit(y)
      cond2 <- nrow(y) <= dlim
      if(cond1|cond2){
        res[i-j] <- NA
        next()
      }
      y <- matrix(y,ncol=1)
      # Create time index
      t <- (1:nrow(y) - nrow(y))/1000
      x <- as.matrix(cbind(1,t,t^2),ncol=3)
      # Get regression coefficients
      gamma <- ols(matrix(y,ncol=1),x)[3]
      res[i-j] <- gamma
    }
    res_list[[k]] <- res
  }
  names(res_list) <- colnames(df)
  res_list <- xts(do.call(cbind,res_list),index(df[eps[-(1:j)],]))
}
