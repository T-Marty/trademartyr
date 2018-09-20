#' Function to form panel data from a portfolio equity matrix. A precursor 
#' to performing robust regression between portfolio legs.
panelise_rp <- function(rpmat,ind_mat=NULL,to_monthly=TRUE){
  if(is.null(names(rpmat))){names(rpmat) <- paste(1:ncol(rpmat))}
  inames <- names(ind_mat)
  
  if(sum(class(ind_mat)=="data.frame")==0){
    tm <- index(ind_mat)
    ind_mat <- as.data.frame(ind_mat)
    ind_mat$date <- tm
    if(to_monthly){
      ind_mat$date <- zoo::as.yearmon(ind_mat$date)
    }
  }
  
  rp_list <- list()
  for (i in 1:ncol(rpmat)){
    x <- na.omit(rpmat[,i])
    if(to_monthly){
      x <- quantmod::monthlyReturn(x)
      index(x) <- as.yearmon(index(x))
    }
    names(x) <- "rp"
    tm <- index(x)
    x <- as.data.frame(x,row.names = FALSE)
    x$date <- tm
    x$portfolio <- names(rpmat)[i]
    x <- merge(x,ind_mat,by="date")
    rp_list[[i]] <- x[,c("date",names(x)[names(x)!="date"])]
  }
  rp_list <- do.call(rbind,rp_list)
  return(rp_list)
}

#' Function to load Fama French 3 factor data
load_fama_french <- function(ffdir,as_df=TRUE){
  ff3 <- readr::read_csv(ffdir,skip = 3,n_max = 1104)
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
cross_quantile_table <- function(Vars,groupings,to_monthly=TRUE,diff=TRUE,
                                 verbose=TRUE){
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
    if(diff){ # Get long-short weights
      x <- get_weights_mr(ranks,long_g = c(i,1),short_g=c(i,groupings[2]))
      weight_list[[l]] <- x
      names(weight_list)[l] <- paste0(names(Vars)[1],i,names(Vars)[2],"1-",
                                      groupings[2])
      l <- l+1
    }
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
    rp <- xts(rowSums(rp),order.by = index(rp))
    rp_list[[w]] <- rp
  }
  # Convert to monthly returns
  if(to_monthly){
    rp_list <- lapply(rp_list,function(x) quantmod::monthlyReturn(na.omit(x)))
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
  ret_mat <- matrix(print_list,ncol=groupings[1],byrow = FALSE)
  colnames(ret_mat) <- paste0(names(Vars)[1],1:groupings[1])
  rownames(ret_mat) <- c(paste0(names(Vars)[2],1:groupings[2]),
                         paste0(names(Vars)[2],"1","-",names(Vars)[2],
                                groupings[2]))
  if(verbose){
    names(mean_list) <- names(t_list) <- names(p_list) <- names(weight_list)
    return(list(res_table=ret_mat,means=mean_list,stat=t_list,ps=p_list,
                portfolios=rp_list))
  }
  return(ret_mat)
}

#' Function to calculate and tabulate the mean return for every quantile 
#' portfolio in a single-sorted portfolio procedure.
inter_quantile_table <- function(Vars,groupings,to_monthly=TRUE,diff=TRUE,
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
      initEq*rp
    }
    stopCluster(mycluster)
    names(out) <- paste0("portfolio",1:K_m)
    rp <- do.call(cbind,out)
    rp <- xts(rowSums(rp),order.by = index(rp))
    rp_list[[w]] <- rp
  }
  # Convert to monthly returns
  if(to_monthly){
    rp_list <- lapply(rp_list,function(x) quantmod::monthlyReturn(na.omit(x)))
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

#' Function to display the mean, t-statistic and p-vals for each column of a 
#' time series as a table.
mean_return_table <- function(comp_mat,sig=4){
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

# Function to get the mean buy-and-hold (portfolio) response to an event,
# as opposed to the continuously equal-weighted response of portfolio firms.
event_response_bh <- function(weights,df_rets,horizon,on="months",stop_loss=NULL){
  cols <- max(diff(endpoints(df_rets,on=on,k=1)))*horizon
  col_min <- min(diff(endpoints(df_rets,on=on,k=6)))
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
  x <- as.data.frame(na.omit(cbind(l,s,ls_)))
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
  v_name <- var_names[1]
  udates <- sort(unique(pmat$date))
  df_list <-list()
  for (i in 1:length(udates)){
    subdf <- filter(pmat,date==udates[i])
    fl <- lm(fmla,subdf)
    subdf[complete.cases(subdf[,var_names]),
          paste0(v_name,"_res")] <- fl$residuals
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
    d_res <- tidyr::spread(df[,c(Index,paste0(v_name,"_res"))],
                           id,paste0(v_name,"_res"))
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