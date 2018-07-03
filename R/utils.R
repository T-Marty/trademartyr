
#' Function to read csv files as xts objects.
#'
#' @param dir The directory in which the required file is stored (string)
#' @param fname name of file to be loaded (string)
#' @param index.col Column name to be referenced for ordering xts object
#' (string). Must be coercible to POSIXct.
#' @param daterange The date range of resulting xts to be loaded (string).
#' @return An xts object.
#' @export
readxts2 <- function(dir, fname, index.col="Date", daterange=NULL, sep=","){
  x <- read.csv(file.path(dir,fname),header=TRUE,sep=sep)
  index.col <- names(x)[grep(index.col,names(x),ignore.case = TRUE)]
  x[[index.col]] <- as.POSIXct(x[[index.col]])
  x<-as.xts(x[,which(names(x) != index.col)],order.by=x[[index.col]])
  indexFormat(x)<-"%Y-%m-%d"
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
                      envir=globalenv(), sep=","){
  for (i in 1:length(fnames)){
    x <- readxts2(dir,fnames[i], index.col = index.col,
                  daterange=daterange, sep=sep)
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
naFillFrom <- function(df, firstDay=FALSE, fromNext=FALSE, ndays=2, ind=NULL,
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
  master_ind <- seq(as.Date(start_date), as.Date(end_date), by=freq)
  master_ind <- master_ind[
    !((weekdays(master_ind) == "Saturday") |
        (weekdays(master_ind) == "Sunday"))]
  master_ind <- master_ind[!(master_ind %in% as.Date(
    timeDate::holidayNYSE(2003:2018)))]
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
clean_hist_members <- function(hist_members){
  hist_members <- readRDS(file.path(compDir,'HM_new.RDS'))
  names(hist_members)[which(names(hist_members)=='CBRE US')] <- "CBG US"
  names(hist_members)[which(names(hist_members)=='WELL US')] <- "HCN US"
  names(hist_members)[which(names(hist_members)=='BKNG US')] <- "PCLN US"
  sp500 <- gsub("/","_",names(hist_members))
  sp500 <- gsub(" US","",sp500)
  names(hist_members) <- sp500
  hist_members <- hist_members[,-(which(sp500=="1437355D"))] # Empty data
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
#' @example
run_parallel <- function(n, ptext, chunk_size=NULL){
  cores <- detectCores()
  if (is.null(chunk_size)){
    chunk_size=trunc(n/(detectCores()-1))
  } else if (chunk_size >= n){
    chunk_size=trunc(n/(detectCores()-1))
  }
  nums1 <- seq(1,n,chunk_size)
  nums2 <- nums1-1
  nums2 <- c(nums2[-1],n)
  mycluster <- makeCluster(cores-1,type = "FORK")
  registerDoParallel(mycluster)
  out <- foreach(z = 1:length(nums1)) %dopar% { # Begin iteration for chunk z
    i_1 <- nums1[z]
    i_2 <- nums2[z]
    eval(parse(text=ptext))
  }
  stopCluster(mycluster)
  return(out)
}

#' Function to extract and neatly present the transactions of a list of
#' portfolios loaded into the environment.
txn_pretty <- function(portfolio_list){
  txn_master_list <- list()
  txn_list <- list()
  for (k in 1:length(portfolio_list)){
    assets <- names(getPortfolio(portfolio_list[k])$symbols)
    for (i in 1:length(assets)){
      sym <- assets[i]
      txns <- getTxns(portfolio_list[k], sym)
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
                                        verbose=FALSE){
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
    from = as.Date(index(weights[i,]))+1
    if (i == nrow(weights)){
      to = as.Date(last(index(df)))
    } else {
      to = as.Date(index(weights[(i+1),]))
    }
    drange <- paste0(from, "::", to)
    returns = df[drange]
    cum_rets <- (cumprod(1+returns))
    for (j in 1:ncol(cum_rets)){
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
