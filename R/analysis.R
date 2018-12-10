#' Function to form panel data from a portfolio equity matrix. A precursor
#' to performing robust regression between portfolio legs.
panelise_rp <- function(rpmat,ind_mat=NULL,to_monthly=TRUE, leading=FALSE){
  if(is.null(names(rpmat))){names(rpmat) <- paste(1:ncol(rpmat))}
  if(!is.null(ind_mat)){
    inames <- names(ind_mat)
    if(sum(class(ind_mat)=="data.frame")==0){
      tm <- index(ind_mat)
      ind_mat <- as.data.frame(ind_mat)
      ind_mat$date <- tm
      if(to_monthly){
        ind_mat$date <- zoo::as.yearmon(ind_mat$date)
      }
    }
  }
  rp_list <- list()
  for (i in 1:ncol(rpmat)){
    x <- zoo::na.trim(rpmat[,i])
    if(to_monthly){
      x <- quantmod::monthlyReturn(x,leading=leading)
      index(x) <- as.yearmon(index(x))
    } else{
      x <- PerformanceAnalytics::Return.calculate(x,method = "discrete")
    }
    names(x) <- "rp"
    tm <- index(x)
    x <- as.data.frame(x,row.names = FALSE)
    if(grepl("POSIX",class(tm)[1])){tm <- as.Date(tm)}
    x$date <- tm
    x$portfolio <- names(rpmat)[i]
    if(!is.null(ind_mat)){
      x <- merge(x,ind_mat,by="date")
    }
    rp_list[[i]] <- x[,c("date",names(x)[names(x)!="date"])]
  }
  rp_list <- do.call(rbind,rp_list)
  return(na.omit(rp_list))
}

#' Function to load Fama French 3 factor data
load_fama_french <- function(ffdir,as_df=TRUE,freq="monthly"){
  if(freq=="daily"){
    ff3 <- readr::read_csv(ffdir,skip = 3)
    names(ff3)[1] <- "date"
    names(ff3)[grep("Mkt-",names(ff3),ignore.case = TRUE)] <- "Mkt_RF"
    ff3$date <- ymd(ff3$date)
    ff3 <- xts(ff3[,names(ff3)!="date"],order.by = ff3$date)
    return(ff3/100)
  }
  ff3 <- readr::read_csv(ffdir,skip = 3,n_max = 663)
  names(ff3)[1] <- "date"
  names(ff3)[grep("Mkt-",names(ff3),ignore.case = TRUE)] <- "Mkt_RF"
  # append day to %Y%m format to allow conversion
  # to date
  ff3$date <- paste0(ff3$date,"01")
  ff3$date <- lubridate::ymd(ff3$date)
  ff3[,names(ff3)!="date"] <- ff3[,names(ff3)!="date"]/100
  if (as_df) {
    ff3$date <- zoo::as.yearmon(ff3$date)
    return(ff3)}
  ff3 <- xts(ff3[,names(ff3)!="date"],order.by = ff3$date)
  index(ff3) <- zoo::as.yearmon(index(ff3))
  return(ff3)
}

#' Function to calculate and tabulate the mean return for every quantile
#' portfolio in a double-sorted portfolio formation procedure. The returns
#' to a long-short portfolio of the most extreme second layer quantiles within
#' each first-layer quantile is also provided.
cross_quantile_table <- function(Vars,groupings,K_m,hhll=FALSE,DF_rets,
                                 verbose=TRUE,remove_period=FALSE,date1=NULL,
                                 date2=NULL,whole_months=TRUE){
  # Get groupings/ranks
  ranks <- multi_rank(Vars=Vars, groupings = groupings)
  # Get weight matrices for each group
  weight_list <- list()
  l=1
  for (i in 1:groupings[1]){
    for (j in 1:groupings[2]){
      x <- get_weights_mr(ranks,long_g = c(i,j),long_only = TRUE)
      weight_list[[l]] <- x
      names(weight_list)[l] <- paste0(names(Vars)[1],i,names(Vars)[2],j)
      l <- l+1
    }
    # Get long-short weights
    x <- get_weights_mr(ranks,long_g = c(i,1),short_g=c(i,groupings[2]))
    weight_list[[l]] <- x
    names(weight_list)[l] <- paste0(names(Vars)[1],
                                    i,names(Vars)[2],"1-",names(Vars)[1],i,
                                    names(Vars)[2],groupings[2])
    l <- l+1
  }
  if(hhll){
    #get_extremes
    x <- get_weights_mr(ranks,long_g=c(1,1),
                        short_g=c(groupings[1],groupings[2]))
    weight_list[[l]] <- x
    names(weight_list)[l] <- paste0(names(Vars)[1],"1",
                                    names(Vars)[2],"1-",names(Vars)[1],
                                    groupings[1],names(Vars)[2],groupings[2])
  }
  # Perform simulations for each weight matrix
  rp_list <- list()
  for (w in 1:length(weight_list)){
    weights=weight_list[[w]]
    cores <- detectCores()
    mycluster <- makeCluster(cores-1,type = "FORK")
    registerDoParallel(mycluster)
    out <- foreach(start_i = 1:K_m) %dopar% {
      wts_i <- weights_i(weights, start_i, K_m, holding_time=FALSE )[[1]]
      rp <- rp_cum_rets(wts_i,DF_rets,stop_loss = 0.9)
      initEq*rp
    }
    stopCluster(mycluster)
    names(out) <- paste0("portfolio",1:K_m)
    rp <- do.call(cbind,out)
    #rp <- xts(rowSums(rp),order.by = index(rp))
    rp_list[[w]] <- rp
  }
  if(remove_period){
    for (i in 1:length(rp_list)){
      rp_list[[i]] <- gfc_adjust(rp_list[[i]],weights=weight_list[[i]],
                                 K_m=K_m,date1,date2,whole_months)
    }
  }
  # Get mean monthly stats
  tab <- sapply(rp_list,mean_return,leading=TRUE)
  ps <- tab[3,]
  tab[1:2,] <- format(round(tab[1:2,],4),scientific = FALSE)
  tab <- apply(tab,2,FUN=function(x) sub(".*? (.+)", "\\1",x))
  tab[1,ps < 0.05] <- paste0(tab[1,ps < 0.05],"*")
  tab[1,ps < 0.01] <- paste0(tab[1,ps < 0.01],"*")
  tab[1,ps < 0.001] <- paste0(tab[1,ps < 0.001],"*")
  tab[1,] <- paste0(tab[1,]," (",tab[2,],")")
  if(hhll){
    hh_ll <- tab[1,ncol(tab)]
    tab <- tab[,-ncol(tab)]
  }
  tab <- matrix(tab[1,],nrow=groupings[1],byrow=TRUE)
  colnames(tab) <- paste(1:ncol(tab))
  colnames(tab)[1:groupings[2]] <- paste0(names(Vars)[2],1:groupings[2])
  colnames(tab)[groupings[2]+1] <- paste0(colnames(tab)[1],"-",
                                            colnames(tab)[groupings[2]])
  rownames(tab) <- paste0(names(Vars)[1],1:nrow(tab))
  if(hhll){
    tab <- rbind(tab,c(hh_ll,rep("",ncol(tab)-1)))
    rownames(tab)[nrow(tab)] <- paste0("11-",groupings[1],groupings[2])
  }
  if(verbose){
    names(rp_list) <- names(weight_list)
    return(list(res_table=tab, portfolios=rp_list))
  }
  return(tab)
}

#' Function to calculate and tabulate the mean *equity* return for every quantile
#' portfolio in a single-sorted portfolio procedure.
inter_quantile_table_eq <- function(Vars,groupings,DF_rets,to_monthly=TRUE,diff=TRUE,
                                 verbose=FALSE){
  # Get groupings/ranks
  ranks <- multi_rank(Vars=Vars, groupings = groupings)
  # Get weight matrices for each group
  weight_list <- list()
  v1 <- ranks[[1]]
  l=1
  for (i in 1:groupings[1]){
    lo <- v1[index(v1), ] == i
    lo[index(lo),] <- as.numeric(lo)
    lo <- na.fill(lo/rowSums(lo),0)
    weight_list[[l]] <- lo
    l <- l+1
  }
  if(diff){ # Get long-short weights
    x <- weight_list[[1]]-weight_list[[groupings[1]]]
    weight_list[[l]] <- x
  }
  # Perform simulations for each weight matrix
  rp_list <- list()
  for (w in 1:length(weight_list)){
    weights=weight_list[[w]]
    cores <- detectCores()
    mycluster <- makeCluster(cores-1,type = "FORK")
    registerDoParallel(mycluster)
    out <- foreach(start_i = 1:K_m) %dopar% {
      wts_i <- weights_i(weights, start_i, K_m, holding_time=FALSE )[[1]]
      rp <- rp_cum_rets(wts_i,DF_rets,stop_loss = 0.9)
      rp
    }
    stopCluster(mycluster)
    names(out) <- paste0("portfolio",1:K_m)
    rp <- do.call(cbind,out)
    rp <- xts(rowSums(rp),order.by = index(rp))
    rp_list[[w]] <- rp
  }
  # Convert to monthly returns
  if(to_monthly){
    rp_list <- lapply(rp_list,
                      function(x) quantmod::monthlyReturn(na.omit(x),
                                                          leading=FALSE))
  }
  rp_list <- do.call(cbind,rp_list)
  names(rp_list) <- names(weight_list)
  # Get portfolio statistics
  mean_list <- colMeans(rp_list)
  sd_list <- apply(rp_list,2,sd)/sqrt(nrow(rp_list))
  t_list <- abs(mean_list)/sd_list
  p_list <- (1-pt(t_list,df=nrow(rp_list)-1))*2
  # Create table
  print_list <- paste0(format(round(mean_list,4),scientific=FALSE),
                       " ", "(", format(round(t_list,4),scientific = FALSE),")")
  print_list[p_list<0.05] <- paste0(print_list[p_list<0.05],"*")
  print_list[p_list<0.01] <- paste0(print_list[p_list<0.01],"*")
  ret_mat <- matrix(print_list,nrow = 1)
  colnames(ret_mat) <- c(paste0(names(Vars)[1],1:groupings[1]),
                         paste0(names(Vars),"1","-",names(Vars),groupings[1]))
  if(verbose){
    names(mean_list) <- names(t_list) <- names(p_list) <- colnames(ret_mat)
    return(list(res_table=ret_mat,means=mean_list,stat=t_list,ps=p_list,
                portfolios=rp_list))
  }
  return(ret_mat)
}

#' Function to calculate and tabulate the mean return for every quantile
#' portfolio in a single-sorted portfolio procedure.
inter_quantile_table <- function(Vars,groupings,K_m,to_monthly=TRUE,diff=TRUE,
                                    verbose=FALSE,stop_loss=0.9,DF_rets,
                                 remove_period=FALSE,date1=NULL,date2=NULL,
                                 whole_months=TRUE){
  # Get groupings/ranks
  ranks <- multi_rank(Vars=Vars, groupings = groupings)
  # Get weight matrices for each group
  weight_list <- list()
  v1 <- ranks[[1]]
  l=1
  for (i in 1:groupings[1]){
    lo <- v1[index(v1), ] == i
    lo[index(lo),] <- as.numeric(lo)
    lo <- na.fill(lo/rowSums(lo),0)
    weight_list[[l]] <- lo
    l <- l+1
  }
  if(diff){ # Get long-short weights
    x <- weight_list[[1]]-weight_list[[groupings[1]]]
    weight_list[[l]] <- x
  }
  # Perform simulations for each weight matrix
  rp_list <- list()
  for (w in 1:length(weight_list)){
    weights=weight_list[[w]]
    cores <- detectCores()
    mycluster <- makeCluster(cores-1,type = "FORK")
    registerDoParallel(mycluster)
    out <- foreach(start_i = 1:K_m) %dopar% {
      wts_i <- weights_i(weights, start_i, K_m, holding_time=FALSE )[[1]]
      rp <- rp_cum_rets(wts_i,DF_rets,stop_loss = stop_loss)
      rp
    }
    stopCluster(mycluster)
    names(out) <- paste0("portfolio",1:K_m)
    rp <- do.call(cbind,out)
    rp_list[[w]] <- rp
  }
  # Remove outlier periods (e.g, GFC) if applicable.
  if(remove_period){
    for (i in 1:length(rp_list)){
      rp_list[[i]] <- gfc_adjust(rp_list[[i]],weights=weight_list[[i]],
                                 K_m=K_m,date1,date2,whole_months)
    }
  }
  # Get mean monthly stats
  tab <- mean_return_table(rp_list,leading = TRUE)
  tab <- data.frame(t(tab),stringsAsFactors = FALSE)
  # remove p-val
  tab[,2] <- sub(".*? (.+)", "\\1", tab[,2])
  tab[,1] <- paste(tab[,1],tab[,2])
  tab <- data.frame(tab[,1])
  rownames(tab) <- paste(1:nrow(tab))
  rownames(tab)[1:groupings[1]] <- paste0(names(Vars)[1],1:groupings[1])
  if(diff){rownames(tab)[groupings[1]+1] <- paste0(rownames(tab)[1],"-",
                                                   rownames(tab)[groupings[1]])}
  colnames(tab) <- "mean (t-stat)"
  if(verbose){
    return(list(res_table=tab, portfolios=rp_list))
  }
  return(tab)
}

#' Function to display the mean, t-statistic and p-vals for each column of a
#' equity time series as a table.
mean_return_table_eq <- function(comp_mat,sig=4){
  nms <- names(comp_mat)
  mean_list <- colMeans(comp_mat)
  sd_list <- apply(comp_mat,2,sd)/sqrt(nrow(comp_mat))
  t_list <- abs(mean_list/sd_list)
  p_list <- (1-pt(t_list,df=nrow(comp_mat)-1))*2
  # Construct strings for printing
  pt_list <- paste0("(",format(round(t_list,sig),scientific=FALSE),")")
  pp_list <- format(round(p_list,sig),scientific=FALSE)
  pt_list <- paste(pp_list,pt_list)
  pt_list[p_list<0.05] <- paste0(pt_list[p_list<0.05],"*")
  pt_list[p_list<0.01] <- paste0(pt_list[p_list<0.01],"*")
  mean_list <- format(round(mean_list,sig),scientific = FALSE)
  # Create table
  table1 <- matrix(c(mean_list,pt_list),byrow=TRUE,
                   ncol=ncol(comp_mat))
  colnames(table1) <- nms
  rownames(table1) <- c("Mean","p (t-stat)")
  as.data.frame(table1)
}

#' Function to display the mean, t-statistic and p-vals for each column of a
#' equity time series as a table.
mean_return_table <- function(rps,verbose=FALSE,sig=4,leading=FALSE){
  nms <- names(rps)
  if(is.null(nms)){nms <- paste0("P",1:length(rps))}
  tab <- sapply(rps,mean_return,leading=leading)
  ps <- tab[3,]
  tab <- format(round(tab,4),scientific = FALSE)
  tab <- apply(tab,2,stringr::str_trim)
  tab[3,] <- paste0(tab[3,]," (",tab[2,],")")
  tab[1,ps<0.05] <- paste0(tab[1,ps<0.05],"*")
  tab[1,ps<0.01] <- paste0(tab[1,ps<0.01],"*")
  tab[1,ps<0.001] <- paste0(tab[1,ps<0.001],"*")
  tab <- tab[-2,]
  tab <- matrix(tab,ncol=length(rps))
  rownames(tab) <- c("mean","p (t-stat)")
  colnames(tab) <- nms
  if(verbose){
    num=t(sapply(rps,mean_return,leading=leading))
    colnames(num) <- c("mean","t-stat","p-val")
    return(list(res_mat=data.frame(tab),num=num))
  }
  return(as.data.frame(tab))
}

# Function to get the mean buy-and-hold (portfolio) response to an event,
# as opposed to the continuously equal-weighted response of portfolio firms.
event_response_bh <- function(weights,df_rets,horizon,on="months",stop_loss=NULL){
  cols <- max(diff(endpoints(df_rets,on=on,k=1)))*horizon
  col_min <- min(diff(endpoints(df_rets,on=on,k=horizon)))
  long_mat <- xts(matrix(ncol=cols,nrow=nrow(weights)),order.by=index(weights))
  short_mat <- long_mat
  ls_mat <- long_mat
  panel_mat <- long_mat[,1:3]
  names(panel_mat) <- c("long_side","short_side","portf")
  if(on=="weeks"){
    f <- weeks
  } else if (on=="months"){
    f <- months
  }
  for (i in 1:nrow(weights)){
    if(sum(weights[i,] != 0)==0){next()}
    sdate <- as.Date(index(weights)[i])
    edate <- sdate %m+% f(horizon)
    edate <- ceiling_date(edate,"months")-days(1)
    if(edate > (index(df_rets)[nrow(df_rets)]+days(5))){break()}
    sdate <- sdate+days(1)
    drange <- paste0(sdate,"::",edate)
    lm <- 0
    sm <- 0
    wts_sum_l=0
    wts_sum_s=0
    long_syms <- names(weights)[weights[i,] > 0]
    short_syms <- names(weights)[weights[i,] < 0]
    if(length(short_syms)!=0){
      short_wts <- as.numeric(abs(weights[i,short_syms]))
      wts_sum_s <- sum(short_wts)
      sm <- apply(cumprod(1+df_rets[drange,short_syms]),1,
                  weighted.mean,w=short_wts) # Buy and hold
      sm <- -(sm)
    }
    if(length(long_syms)!=0){
      long_wts <- as.numeric(abs(weights[i,long_syms]))
      wts_sum_l <- sum(long_wts)
      lm <- apply(cumprod(1+df_rets[drange,long_syms]),1,
                  weighted.mean,w=long_wts) # Buy and hold
    }
    lsm <- wts_sum_l*lm+wts_sum_s*sm
    if(!is.null(stop_loss)){
      if(min(lsm)<=-1*stop_loss){
        wls <- which(lsm <= -1*stop_loss)[1]
        hold_ret_ls <- lsm[wls]
        lsm[(wls+1):length(lsm)] <- NA
        sm[(wls+1):length(sm)] <- NA
        lm[(wls+1):length(lm)] <- NA
        hold_ret_l <- (tail(na.omit(lm),1)-1)*wts_sum_l
        hold_ret_s <- (tail(na.omit(sm),1)+1)*wts_sum_s
      }
    } else{
      hold_ret_l <- (tail(lm,1)-1)*wts_sum_l
      hold_ret_s <- (tail(sm,1)+1)*wts_sum_s
      hold_ret_ls <- tail(lsm,1)
    }
    sdate <- as.Date(index(weights)[i])
    long_mat[sdate,1:length(lm)] <- lm*wts_sum_l - wts_sum_l
    short_mat[sdate,1:length(sm)] <- sm*wts_sum_s + wts_sum_s
    ls_mat[sdate,1:length(lsm)] <- lsm + (wts_sum_s-wts_sum_l)
    panel_mat[sdate,1:3] <- c(hold_ret_l,hold_ret_s,hold_ret_ls)
  }
  l <- c(0,colMeans(long_mat,na.rm = TRUE))
  s <- c(0,colMeans(short_mat,na.rm = TRUE))
  ls_ <- c(0,colMeans(ls_mat,na.rm = TRUE))
  x <- as.data.frame(cbind(l,s,ls_))
  names(x) <- c("long","short","long_short")
  x$days <- 1:nrow(x)
  x <- x[1:col_min,]
  return(list(event_mat=x,panel=panel_mat,
              long_mat=long_mat,short_mat=short_mat,ls_mat=ls_mat))
}

#' Function to get the residual series for a given period-by-period regression
#' equation and append to existing data set or flatten to time series for
#' ranking.
create_residual <- function(pmat,fmla,flatten=TRUE,
                            id="ticker",ind,assets=NULL){
  # Get relevant variable names for complete.cases
  var_names <- intersect(vnames(fmla),names(pmat))
  # Make name for residual variable and make sure not taken
  v_name <- paste0(var_names[1],"_res")
  while(v_name %in% names(pmat)){
    v_name <- paste0(v_name,"x")
  }
  udates <- sort(unique(pmat$date))
  df_list <-list()
  for (i in 1:length(udates)){
    subdf <- dplyr::filter(pmat,date==udates[i])
    fl <- lm(fmla,subdf)
    subdf[complete.cases(subdf[,var_names]),v_name] <- fl$residuals
    df_list[[i]] <- subdf
  }
  df <- do.call(rbind,df_list)
  rm(df_list)
  if (flatten){
    # Find time column
    tcol=which(sapply(1:ncol(df),
                      FUN=function(x) class(df[,x])) %in%
                 c("Date","POSIXct","POSIXt","yearmon"))[1]
    # Subset for spreading
    Index=c(names(df)[tcol],id)
    # Spread the residual data into format used for ranking and position prep
    d_res <- tidyr::spread(df[,c(Index,v_name)],id,v_name)
    # Assets with less than J_m periods of data will not be in the panel data.frame
    if(!is.null(assets)){
      no_data <- assets[!(assets %in% names(d_res))]
      # Create empty data set for these assets
      cmat <- matrix(nrow = nrow(d_res),ncol = length(no_data),
                     dimnames = list(NULL,no_data))
      # Merge with rest of data
      d_res <- cbind(d_res,cmat)
      # Put columns in "correct" order (to match 'assets' i.e. to match other objects)
      d_res <- xts(d_res[,assets],order.by=d_res[,Index[1]])
    } else {
      d_res <- xts(d_res[,-which(names(d_res)==Index[1])],
                   order.by=d_res[,Index[1]])
    }
    # Align index
    if(!is.null(ind)){
      d_res <- cbind(d_res,ind[endpoints(ind,"months",k=1)])
    }
    if(!is.null(assets)){
      names(d_res) <- assets
    }
    return(list(panel=df,flat=d_res))
  }
  return(df)
}

#' Function to extract variable names from an equation.
vnames <- function(f){
  f <- as.character(f)
  f <- paste(f,collapse = " ")
  f <- gsub("~"," ",f,fixed = TRUE)
  f <- gsub("+"," ",f,fixed = TRUE)
  f <- gsub("-"," ",f,fixed = TRUE)
  f <- gsub("*"," ",f,fixed = TRUE)
  f <- gsub("I("," ",f,fixed = TRUE)
  f <- gsub("("," ",f,fixed = TRUE)
  f <- gsub(")"," ",f,fixed = TRUE)
  f <- gsub("^"," ",f,fixed = TRUE)
  f <- gsub("/"," ",f,fixed = TRUE)
  f <- stringr::str_squish(f)
  f <- unlist(strsplit(f,split = " "))
  return(unique(f))
}

#' Function to append the sorting group of an asset for a given variable
#' (or combination of variables) to panel data.
append_group <- function(pmat,Vars,groupings,Id="ticker"){
  # Elements of Vars should be ranks, not raw variables!
  if(is.null(names(Vars))){ names(Vars) <- paste0("V",1:length(Vars))}
  mr <- multi_rank(Vars=Vars,groupings=groupings)
  mr <- mr[[length(mr)]]
  tm <- index(mr)
  mr <- as.data.frame(mr,row.names = FALSE)
  mr$date <- tm
  mr <- tidyr::gather(mr,ticker,group,-date)
  names(mr)[which(names(mr)=="ticker")] <- Id
  names(mr)[which(names(mr)=="group")] <- paste0(names(Vars),"_group")
  mr <- merge(pmat,mr,all.x = TRUE)
}

#' Function to get different types of mean return from portfolio return object.
mean_return <- function(rpmat,overlapping=TRUE,leading=FALSE){
  rp <- lapply(1:ncol(rpmat),
               FUN=function(x) quantmod::monthlyReturn(na.omit(rpmat[,x]),
                                                       leading=leading))
  rp <- do.call(cbind,rp)
  if (overlapping){
    rp <- na.omit(rp)
    ts_mean <- rowMeans(rp)
    n <- length(ts_mean)
    t_m <- mean(ts_mean)
    s_d <- sd(ts_mean)/sqrt(n)

  } else {
    rp <- na.omit(as.numeric(rp))
    t_m <- mean(rp)
    n <- length(rp)
    s_d <- sd(rp)/sqrt(n)
  }
  tstat <- t_m/s_d
  pval <- (1-pt(abs(tstat),df=n-1))*2
  return(cbind(mean=t_m,tstat=tstat,pval=pval))
}

#' Function to remove no-invest periods in portfolio simulation results.
#' @description Trading resumes at next entry date for a given K portfolio.
#' In this ways, portfolios are still staggered after event period.
gfc_adjust <- function(rmat,weights,K_m,date1,date2,whole_months=TRUE,
                       overlapping_only=FALSE){
  date1 <- as.Date(date1)
  date2 <- as.Date(date2)
  init_vals <- apply(rmat,2,function(x) as.numeric(na.omit(x)[1]))
  adj_list <- list()
  for (i in 1:ncol(rmat)){
    x <- na.omit(rmat[,i])
    x <- diff(log(x))
    x[1,] <- 0
    wi <- weights_i(weights,start_i=i,k=K_m)[[2]]
    d2i <- index(wi)[which(index(wi)>=date2)[1]]
    d1i <- GFC1
    if(whole_months){d1i <- floor_date(d1i,"months")}
    if(!overlapping_only){
      x[index(x) >= d1i & index(x) <= d2i] <- 0
      x <- exp(cumsum(x))*init_vals[i]
    }
    x[index(x) >= d1i & index(x) <= d2i] <- NA
    adj_list[[i]] <- x
  }
  adj_list <- do.call(cbind,adj_list)
  names(adj_list) <- names(rmat)
  if(overlapping_only){
    # Want to keep original index, but everywhere without full overlap make NA
    tm0 <- index(adj_list)
    tm <- index(na.trim(adj_list))
    adj_list <- na.omit(adj_list)
    adj_list <- cbind(adj_list,tm)
    nas <- which(is.na(adj_list[,1]))
    adj_list <- exp(cumsum(na.fill(adj_list,0)))
    adj_list[nas,] <- NA
    adj_list <- cbind(adj_list,tm0)
  }
  return(adj_list)
}

#' Function to remove weights on assets with discordent information.
#' Specifcally, only long (short) holdings with positive (negative) variables
#' (e.g. sentiment, momentum) are kept.
remove_discordant <- function(weights,Raw){
  # Raw is list of raw variable values (not ranks)
  long <- short <- xts(matrix(0,nrow=nrow(weights),ncol=ncol(weights)),
                       order.by=index(weights))
  long <- (weights>0)+long
  short <- (weights<0)+short
  for (i in 1:length(Raw)){
    if(!is.null(Raw[[i]])){
      long <- (Raw[[i]] > 0)*long
      short <- (Raw[[i]] < 0)*short
    }
  }
  long <- long/rowSums(long,na.rm = TRUE)
  short <- short/rowSums(short,na.rm = TRUE)
  long <- na.fill(long,0)
  short <- na.fill(short,0)
  wts <- long-short
  names(wts) <- names(weights)
  return(wts)
}

#' Function to perform simultaneous multiple sorting. Differs from multi_rank
#' in that sorting is not conducted consecutively. Rather, the ranks of an
#' asset with respect to each variable are multiplied together.
#' Removes assets with discordant information if Raw !=null.
multi_rank_no_order_deprecated <- function(Vars,Raw,long_only=FALSE, TopN){
  # Note: Zero used to identify non-investable assets
  r <- Vars[[1]]
  for (i in 2:length(Vars)){
    r <- r*Vars[[i]]
  }
  long=r
  short=r
  for (i in 1:length(Raw)){
    if(!is.null(Raw[[i]])){
      long <- (Raw[[i]] > 0)*long
      short <- (Raw[[i]] < 0)*short
    }
  }
  long <- na.fill(long,0)
  short <- na.fill(short,0)
  for (i in 1:nrow(r)){
    ranks_l <- rank(as.numeric(long[i,long[i,]!=0]),ties.method="first")
    long[i,long[i,]!=0] <- ranks_l
    ranks_s <- rank(-as.numeric(short[i,short[i,]!=0]),ties.method="first")
    short[i,short[i,]!=0] <- ranks_s
  }
  long[index(long),] <- as.matrix((long <= TopN)&(long>0))
  long <- na.fill(long/rowSums(long),0)
  short[index(short),] <- as.matrix((short <= TopN)&(short>0))
  short <- na.fill(short/rowSums(short),0)
  if(long_only){
    names(long) <- names(Vars[[1]])
    return(long)}
  wts <- (long-short)
  names(wts) <- names(Vars[[1]])
  return(wts)
}

#' Function which combines individual ranks simultaneosly, as opposed to
#' sequentially as in multi_rank.
# Like multi_rank, it is assumed ranks already account for missing data or
#' non-membership with zeros.
multi_rank_no_order <- function(rank_list, type = c("dist","sum","prod")){
  type = match.arg(type)
  if (type=="sum"){
    r <- do.call("+",rank_list)
  } else if (type=="prod"){
    r <- do.call("*",rank_list)
  } else {
    r <- sqrt(do.call("+",lapply(rank_list,function(x) x^2)))
  }
  r <- r*do.call("*",lapply(rank_list,function(x) x != 0))
  for (i in 1:nrow(r)){
    if(sum(r[i,]!=0)==0){next()}
    r[i,r[i,]!=0] <- rank(as.numeric(r[i,r[i,]!=0]),ties.method="first")
  }
  return(r)
}


#' Function to parallelize return-based simulation over each leg.
return_sim <- function(weights,K_m=6,DF_rets,
                       stop_loss=0.9,holding_time=FALSE, initEq=1){
  cores <- detectCores()
  mycluster <- makeCluster(cores-1,type = "FORK")
  registerDoParallel(mycluster)
  out <- foreach(start_i = 1:K_m) %dopar% {
    wts_i <- weights_i(weights, start_i, K_m, holding_time)[[1]]
    rp <- rp_cum_rets(wts=wts_i,DF_rets,stop_loss)
    initEq*rp
  }
  stopCluster(mycluster)
  names(out) <- paste0("portfolio",1:K_m)
  rmat <- do.call(cbind, out) ;rm(out)
  # Make first value in each column initial equity
  for (i in 1:ncol(rmat)){
    last_na <- which(!is.na(rmat[,i]))[1]-1
    rmat[last_na,i] <- initEq
  }
  rp <- xts(rowSums(na.trim(rmat)),order.by = index(na.trim(rmat)))
  return(list(rmat=rmat,rp=rp))
}

#' Function that takes a primary ranking and compromises the primary
#' rank to ensure concordant variable direction of specified variables.
remove_discordant_bruce <- function(primary_ranks,Raw,TopN){
  # Function to use rankings of first variable,
  # but apply variables in Raw as a filter based on sign.
  # i.e. long (short) side only contains assets with positive (negative)
  # Raw values
  long <- short <- primary_ranks
  for (i in 1:length(Raw)){
    if(!is.null(Raw[[i]])){
      long <- (Raw[[i]] > 0)*long
      short <- (Raw[[i]] < 0)*short
    }
  }
  long <- na.fill(long,0)
  short <- na.fill(short,0)
  for (i in 1:nrow(long)){
    ranks_l <- rank(as.numeric(long[i,long[i,]!=0]),ties.method="first")
    long[i,long[i,]!=0] <- ranks_l
    ranks_s <- rank(-as.numeric(short[i,short[i,]!=0]),ties.method="first")
    short[i,short[i,]!=0] <- ranks_s
  }
  short <- ((short <= TopN) & (short > 0))
  long <- ((long <= TopN) & (long > 0))
  short <- na.fill(short/rowSums(short,na.rm = TRUE),0)
  long <- na.fill(long/rowSums(long,na.rm = TRUE),0)
  wts <- long-short
  names(wts) <- names(primary_ranks)
  return(wts)
}

#' Function to run factor regressions and neatly present results.
#' Allows for panel-style or mean overlapping return analysis.
alpha_table <- function(panel_list,factor_mat,model=c("TS","CAPM","FF3","FF5"),
                        method=c("Panel","Fama"),vcov_panel=plm::vcovSCC,
                        vcov_fama=sandwich::NeweyWest,to_monthly=TRUE,
                        leading=FALSE){
  method = match.arg(method)
  model = match.arg(model)
  fmla <- switch(model, CAPM=I(rp-RF)~Mkt_RF, FF3=I(rp-RF)~Mkt_RF+HML+SMB,
                 FF5=I(rp-RF)~Mkt_RF+HML+SMB+RMW+CMA,TS=rp~1)
  if(method=="Panel"){
    # Method 1 - SCC Panel
    alpha1 <- numeric(length(panel_list))
    p1 <- numeric(length(panel_list))
    for (i in 1:length(panel_list)){
      rpmat <- panel_list[[i]]
      rp_panel <- panelise_rp(rpmat,ind_mat = factor_mat,
                              to_monthly=to_monthly,leading=leading)
      reg_model <- plm(fmla,data=rp_panel,index=c("portfolio","date"),
                       model="pooling")
      coefs <- as.matrix(coeftest(reg_model, vcov. = vcov_panel))
      alpha1[i] <- round(coefs[1,1],4)
      p1[i] <- round(coefs[1,4],4)
    }
  } else if(method=="Fama"){
    alpha1 <- numeric(length(panel_list))
    p1 <- numeric(length(panel_list))
    for (i in 1:length(panel_list)){
      rpmat <- zoo::na.trim(panel_list[[i]])
      if(to_monthly){
        out <- lapply(1:ncol(rpmat),
                      FUN=function(x){quantmod::monthlyReturn(rpmat[,x],
                                                              leading=leading)})
        out <- do.call(cbind,out)
        index(out) <- as.yearmon(index(out))
        rpmat <- na.omit(out)
      } else{
        rpmat <- PerformanceAnalytics::Return.calculate(rpmat,"discrete")
      }
      tm <- index(rpmat)
      rpmat <- data.frame(cbind(date=1,rp=rowMeans(rpmat,na.rm = FALSE)))
      rpmat$date <- tm
      rpmat <- merge(rpmat,factor_mat)
      reg_model <- lm(fmla,data=rpmat)
      coefs <- as.matrix(coeftest(reg_model, vcov.=vcov_fama))
      alpha1[i] <- round(coefs[1,1],4)
      p1[i] <- round(coefs[1,4],4)
    }
  }
  alpha1 <- format(alpha1,scientific = FALSE)
  alpha1[p1<0.05] <- paste0(alpha1[p1<0.05],"*")
  alpha1[p1<0.01] <- paste0(alpha1[p1<0.01],"*")
  alpha1[p1<0.001] <- paste0(alpha1[p1<0.001],"*")
  p1 <- format(p1,scientific = FALSE)
  capm_table1 <- data.frame(rbind(intercept=alpha1,paste0("(",p1,")")))
  names(capm_table1) <- names(panel_list)
  return(capm_table1)
}

#' Function for holding period event response from overlapping return matrix.
#' @description Limited function for event plot from overlapping return matrix.
#' Default shows average holding period cumulative return by month. I.e.
#' average of all holding period equity (normalised) stacked on top of
#' one another.
#' If returns=TRUE, function averages holding period return each month - the
#' result then shows the average holding period return if all portfolio
#' holding periods were being held at the same time and equal-weighted
#' rebalanced each month (although the stocks within each holding period) are
#' buy-hold, or whatever the strategy implemented.
#' Made to be used directly from strategy output.
event_from_rmat <- function(rmat,K_m,verbose=FALSE,returns=FALSE,
                            index_at="endof",leading=FALSE){
  res_list <- vector('list',ncol(rmat))
  if(returns){
    for (i in 1:ncol(rmat)){
      x <- na.omit(rmat[,i])
      #x <- to.monthly(x,indexAt = "endof")
      #x <- x$x.Close
      x <- na.omit(quantmod::monthlyReturn(x,leading = leading))
      res_mat <- data.frame(matrix(nrow=ceiling(nrow(x)/K_m),ncol=(K_m+2)))
      names(res_mat) <- c("date",paste0("m",0:K_m))
      res_mat$m0 <- 0
      for (j in 1:ceiling(nrow(x)/K_m)){
        rpsub <- x[(K_m*j-K_m+1):min((K_m*j),nrow(x))]
        res_mat$date[j] <- as.Date(index(rpsub)[1])
        res_mat[j,paste0("m",1:nrow(rpsub))] <- as.numeric(as.numeric(rpsub))
      }
      res_list[[i]] <- res_mat
    }
    res_list <- do.call(rbind,res_list)
    if(verbose){
      return(res_list)
    }
    return(colMeans(res_list[,-1],na.rm = TRUE))
  } else{
    for (i in 1:ncol(rmat)){
      x <- rmat[,i]
      x <- na.omit(x)
      x <- xts::to.monthly(x,indexAt = index_at)
      x <- x$x.Close
      res_mat <- data.frame(matrix(nrow=ceiling(nrow(x)/K_m),ncol =(K_m+2)))
      names(res_mat) <- c("date",paste0("m",0:K_m))
      for (j in 1:ceiling(nrow(x)/K_m)){
        rpsub <- x[(K_m*j+1-K_m):min(((K_m*j+1)),nrow(x))]
        rpsub <- rpsub/as.numeric(rpsub[1,1])
        res_mat$date[j] <- as.Date(index(rpsub)[1])
        res_mat[j,paste0("m",0:(nrow(rpsub)-1))] <- as.numeric(as.numeric(rpsub))
      }
      res_list[[i]] <- res_mat
    }
    res_list <- do.call(rbind,res_list)
    if(verbose){
      return(res_list)
    }
    return(colMeans(res_list[,-1],na.rm = TRUE)-1)
  }
}

#' Thompson (2011) Estimator for two-way clustering plus spatial correlation.
vcovThompson <- function(x, maxlag = NULL, ...) {
  w1 <- function(j, maxlag) 1
  VsccL.1 <- vcovSCC(x, maxlag = maxlag, wj = w1, ...)
  Vcx <- vcovHC(x, cluster = "group", method = "arellano", ...)
  VnwL.1 <- vcovSCC(x, maxlag = maxlag, inner = "white", wj = w1, ...)
  return(VsccL.1 + Vcx - VnwL.1)
}

#' High-level sandwhich::kweights wrapper to pass to plm.
kern_weights <- function(j, maxlag=NULL,
                         kern=c("Bartlett","Truncated","Parzen",
                                "Tukey-Hanning","Quadratic Spectral")){
  kern=match.arg(kern)
  if(kern=="Bartlett"){
    return(max((1-j/(maxlag+1)),0))
  }
  b = j/maxlag
  return(sandwich::kweights(b,kern))
}

#' High-level plm::vcovSCC wrapper for Driscoll and Kraay's SCC estimator.
#' Allows easier specification of kernel smoother.
vcovDK <- function(x,maxlag=NULL,kern){
  maxlag <- ifelse(is.null(maxlag),ceiling((max(pdim(x)$Tint$Ti))^(1/4)),maxlag)
  plm::vcovSCC(x, maxlag=maxlag,
               wj=function(j,maxlag){kern_weights(j,maxlag,kern)})
}

#' Function to lead xts objects.
#' Example for calculating forward return:
#' fwd_6 <- lead_xts(x,6)/x -1
lead_xts <- function(df,n){
  df <- as.data.frame(df)
  df <- xts(apply(df,2,dplyr::lead,n=n),as.POSIXct(rownames(df)))
}

#' Function for applying alpha function to subperiods and tabulating.
#' @param rps List of prices (portfolios) to analyse.
#' @param periods List of periods in YYYYmmdd string format,
#' e.g. "20030128::20040101". Use '-' to exclude periods, e.g.
#' "-20030128::20040101".
#' @param func Function to get mean return or alpha, e.g. alpha_table(...)
#' @param str_fun Function to apply to each table. Useful for keeping or removing
#' t-stats or p-vals.
#' @details Example:
#' p1 <- "20030101::20080914"
#' p2 <- "-20080915::20090930"
#' p3 <- "20091001::20171231"
#' periods <- list(p1,p2,p3)
#' rps <- mom_deciles$portfolios[c(1,10,11)]
#' names(rps) <- c("High Mom","Low Mom","HML Mom")
#' func <- function(x) {t(alpha_table(x,model = "TS",method="Panel", factor_mat=ff3,
#'                                    vcov_panel=vcov_panel,leading=TRUE))}
#' str_func1 <- function(x) {gsub(" .*", "", x)} # remove text after space
#' str_func2 <- function(x) {gsub(".* ", "", x)} # remove text before space
#' spts <- sub_period_alphas(lapply(rps,na.omit),periods,func= function(x)
#' {t(mean_return_table(x,leading = TRUE))},str_func = str_func1)
#' spts <- cbind(rep(c("Pre-GFC","Ex-GFC","Post-GFC"),each=2),spts)
sub_period_alphas <- function(rps,periods,func,str_func=NULL){
  if(is.null(names(rps))){
    nms <- paste0("P",1:length(rps))
  } else {
    nms <- names(rps)
  }
  rp_list <- vector('list',length(rps))
  # Create dfs for each sub-period
  for (j in 1:length(rps)){
    rmat <- rps[[j]]
    sub_list <- vector('list',length(periods))
    for(i in 1:length(periods)){
      if(substr(periods[i],1,1)=="-"){
        pd <- lapply(strsplit(gsub("-","",periods[i]),"::"),lubridate::ymd)
        sub_list[[i]] <- rmat[index(rmat)<pd[[1]][1] | index(rmat)>pd[[1]][2],]
      } else {
        sub_list[[i]] <- rmat[periods[[i]]]
      }
    }
    rp_list[[j]] <- sub_list
  }
  # Perform regressions/get alpha
  mrt <- lapply(rp_list,func)
  mrt <- do.call(cbind,mrt)
  if(!is.null(str_func)){
    mrt <- apply(mrt,2,str_func)
  }
  resmat <- matrix("",nrow=length(periods)*2,ncol=length(rps))
  resmat[which(1:nrow(resmat)%%2==1),] <- mrt[,which(1:ncol(mrt)%%2==1)]
  resmat[which(1:nrow(resmat)%%2==0),] <- mrt[,which(1:ncol(mrt)%%2==0)]
  colnames(resmat) <- nms
  #rn <- lapply(periods,
  #             function(x) paste0(ymd(unlist(strsplit(x,"::"))),collapse="/\n"))
  #resmat <-cbind(rep(unlist(rn),each=2),resmat)
  return(resmat)
}
