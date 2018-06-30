#' Function for ranking assets on momentum that require index membership for
#' consideration.
#' @description Function to rank assets based on price momentum, but only
#' consider those that have index membership at *time of ranking*. Function
#' performs ranking on last *trading day* of period. Future versions to include
#' the option to perform rankings at different days of period/month.
#' @param df xts object containing price series of assets to rank. Every column
#' will be treated as an independent asset. Column names of assets to rank must
#' also appear in column names of `hist_members`
#' @param n Formation period e.g. '6' with `on`=months' for 6 month formation
#' period. (integer)
#' @param s Skip period (integer). This is the time between the end of formation
#'  period and ranking (and presumably investment) date).
#' @param hist_members xts object containing membership data. Column names are
#' assumed to be asset names corresponding to those in df. Note: All entries
#' not NA are assumed to represent active membership.
#' @param on Desired return periodicity (string). Passed to xts::endpoints.
#' @note NEED TO REMOVE Automatic NYSE trading days.
#' @importFrom zoo index
#' @export
momRankMembers <- function(df, n=6, s=0, hist_members=newHM, on="months",
                           ind=NULL){
  # must be members in current period, but not at time of roc calc
  # Next step is to put back up momentum if not enough price data is available
  if (is.null(ind)){
    ind <- nyse_trading_dates(as.Date(index(df)[1]),
                              as.Date(index(df)[nrow(df)]))
  }
  eps <- xts::endpoints(ind,on=on)
  eps_dates <- ind[eps]
  roc <- TTR::ROC(df[eps_dates],n=n)
  mom <- as.xts(matrix(0,ncol=ncol(roc),nrow = nrow(roc)),order.by = index(roc))
  names(mom) <- names(roc)
  # Ensure asset names in hist_members only contain those in df
  hist_members <- hist_members[,base::intersect(names(hist_members),names(df))]
  for (i in (n+1+s):length(eps_dates)){
    # Get members
    members <- names(hist_members)[which(!is.na(hist_members[eps_dates[i],]))]
    # Get members with data (i.e. some don't have enough price history for
    # lookback)
    members_with_data <- members[!is.na(as.numeric(roc[eps_dates[i-s],members]))]
    if(length(members_with_data)==0){next()}
    mom[eps_dates[i],members_with_data] <- rank(
      -as.numeric(roc[eps_dates[i-s],members_with_data]),ties.method = "first")
  }
  return(mom)
}

#' Function to get equal-weighted long-short position/weight vector from ranks.
#' @description Takes an xts of ranks and returns an xts object containing
#' position sizes (weights) for an equal weighted long-short portfolio.
#' @param ranks xts object containing asset ranks. Column names are assumed to
#' be independent assets.
#' @param TopN Number of assets on each side (long or short) of trade (integer).
#' Total number of assets held each period will be TopN*2
#' #' @importFrom zoo index
#' @export
get_weights_ls <- function(ranks, TopN){
  #--- Create long position signals
  long <- (ranks[index(ranks), ] <= TopN) & (ranks[index(ranks), ] > 0)
  long[index(long), ] <- as.numeric(long)
  #--- Get number of active (with rank) assets at time of rank
  nassets <- ranks[index(ranks), ] != 0
  nassets <- rowSums(nassets)
  #--- Create short position signals
  short <- (ranks[index(ranks), ] > (nassets-TopN))&(ranks[index(ranks), ] > 0)
  short[index(long),] <- -as.numeric(short)
  #--- Create equal weights
  wts <- (long+short)/TopN
  return(wts)
}

#' Function for getting the final day of asset membership with data.
#' @description Finds the last trading day (with recorded data) for each asset
#' each period (e.g. month).
#' @details Returns a dataframe with sepearate `Date` column. This is last day
#' of the given trading period. The remaining columns are the asset columns.
#' The asset columns contain the last trading date for the given month, for
#' which the asset was a member.
#' @param df xts object containing price series of assets to rank. Every column
#' will be treated as an independent asset. Column names of assets to rank must
#' also appear in column names of `hist_members`
#' @param membership If FALSE will ignore asset membership and return last day
#' of period with data.
#' @param hist_members xts object containing membership data. Column names are
#' assumed to be asset names corresponding to those in df. Note: All entries
#' not NA are assumed to represent active membership.
#' @param on Desired return periodicity (string). Passed to xts::endpoints.
#' Ignored if `ind` is provided.
#' @param ind Optional date series to use as period end points. Used in place
#' of `ind`.
#' @note Needs error for when `ind` doesn't match index(df). If no longer member
#' still want to return last traded day, not just NA. Too slow, only go to
#' last day with data.
#' @importFrom zoo index
#' @export
txn_dates <- function(df, membership=TRUE, hist_members=newHM,
                                 on="months", ind=NULL){
  if(is.null(ind)){
    eps <- xts::endpoints(df,on=on)
    eps[1] <- 1 # New added! If it breaks, try without this!
    ind <- index(df)[eps]
  } else {
    ind <- ind
  }
  dym <- as.data.frame(matrix(NA,nrow=length(ind),ncol=ncol(df)))
  names(dym) <- names(df)
  dym$Date <- ind
  if (membership){
    # Ensure asset names in membership only contain those in df
    hist_members <- hist_members[,
                                 base::intersect(names(hist_members),names(df))]
    for (i in 1:length(ind)){
      if (i==1){
        drange <- paste0(index(df)[1],"/",ind[i])
      } else {drange <- paste0(ind[i-1]+days(1),"/",ind[i])}
      df_sub <- df[drange]
      mem_sub <- hist_members[drange]
      for(j in 1:ncol(df)){
        sym <- names(df)[j]
        # last day with membership and price data
        with_data <- as.Date(index(df_sub)[which(!is.na(df_sub[,sym]))])
        with_membership <- as.Date(index(mem_sub)[which(!is.na(mem_sub[,sym]))])
        commn <- intersect(with_data,with_membership)
        if (length(commn)==0){next()}
        dym[i,sym] <- max(commn)
      }
    }
    for (i in 1:ncol(dym)){
      dym[,i] <- as.Date(dym[,i])
    }
    return(dym)
  } else{
    for (i in 1:length(ind)){
      if (i==1){
        drange <- paste0(index(df)[1],"/",ind[i])
      } else {drange <- paste0(ind[i-1]+days(1),"/",ind[i])}
      df_sub <- df[drange]
      for(j in 1:ncol(df)){
        sym <- names(df)[j]
        with_data <- as.Date(index(df_sub)[which(!is.na(df_sub[,sym]))])
        if (length(with_data)==0){next()}
        dym[i,sym] <- max(with_data)
      }
    }
    for (i in 1:ncol(dym)){
      dym[,i] <- as.Date(dym[,i])
    }
    return(dym)
  }
}

#' Function to simulate an individual (1 of K) momentum portfolio.
#' @description Function to simulate an individual (1 of K) momentum portfolio.
#' This function assumes rebalancing/exits occur on same day of entries,
#' and does not look for exit explict exit/rebalnce signals. The exception is
#' for memberships.
#' @details This procedure assumes transactions all occur on same date, with
#' last available price (otherwise, the last trading date or memberhsip exit
#' date must be known in advance, or sorted within the procedure, since blotter
#' requires transactions to be added in date order).
#' @note If doesn't work in parallel environment, may need to load blotter and
#' financial instrument packages within the function and call `stocks`
#' `currency`. Best to find a way around this, as shouldn't call library within
#' packages.
place_transactions_index <- function(initEq,
                               assets, Currency, ind, start_i=1, K_m=1, wts=wts,
                               initdate=initdate, enddate=enddate,
                               enter_prefer="Close",
                               exit_prefer="Close"){
  #--- Create flag every K months to buy/sell
  p <- which(rowSums(abs(wts)) != 0)[1]
  month_index <- xts(matrix(c(rep(0,p-1),K_m:(nrow(wts)+K_m-p)),ncol=1),
                     order.by=index(wts))
  wt_ind <- month_index - start_i + 1
  wt_ind <- wt_ind[(wt_ind %% K_m == 0) & (wt_ind > 0),]
  wts_i <- wts[index(wt_ind),]
  portfolioName <- paste0("portfolio_",start_i)
  suppressWarnings(rm(list=c(paste0("portfolio.",portfolioName)), pos=.blotter))
  blotter::initPortf(name=portfolioName, symbols=assets, initDate=initdate)
  for( i in 1:length(ind) ) { # Start date loop
    current_date=ind[i]
    drange <- paste0(ind[1],"/",current_date)
    blotter::updatePortf(Portfolio=portfolioName, Dates=drange)
    #Note that the above line will update until end of current_date
    # On dates without entries/rebalances, check all assets are still members
    if (!(current_date %in% index(wt_ind))){
      for (j in 1:length(assets)){
        symbol <- assets[j]
        Posn <- blotter::getPosQty(portfolioName, Symbol = symbol,
                                   Date = current_date)
        if (Posn !=0){
          if (is.na(get(symbol)[current_date,"Member"]) |
              (get(symbol)[current_date,"Member"] !=1)){
            sym <- get(symbol)[paste0("::",current_date)]
            # last available price
            price <- sym[last(which(!is.na(sym[,exit_prefer]))),exit_prefer]
            blotter::addTxn(portfolioName, Symbol=symbol, TxnDate=current_date,
                   TxnPrice=price, TxnQty=-Posn, TxnFees=0)
          }
        }
      }
    }
    if(current_date %in% index(wt_ind)){ # if entry date
      equity<-sum(blotter::dailyStats(portfolioName,use="equity")[1])+(initEq)
      for (j in 1:length(assets)){
        symbol <- assets[j]
        sym <- get(symbol)
        enter_price <- sym[current_date,enter_prefer]
        exit_price <- sym[current_date,exit_prefer]
        Posn <- blotter::getPosQty(portfolioName, Symbol = symbol,
                          Date = current_date)
        entrySig <- wts[current_date,symbol]
        if (Posn != 0){ # If holding position, sell
          if(is.na(exit_price)){
            exit_price <- sym[last(which(!is.na(sym[,exit_prefer]))),exit_prefer]# last available price
          }
          blotter::addTxn(portfolioName, Symbol=symbol, TxnDate=current_date,
                          TxnPrice=exit_price, TxnQty=-Posn, TxnFees=0)
        }
        if ( (entrySig != 0) & holdingTime(current_date,as.Date(enddate),K_m) &
             (equity > 0)){ # 2nd cond to align with bruce
          if(is.na(enter_price)){
            enter_price <- sym[last(which(!is.na(sym[,enter_prefer]))),
                               enter_prefer]# last available price
          }
          qty <- as.numeric(trunc((abs(equity)*wts[current_date,symbol])/enter_price))
          blotter::addTxn(portfolioName, Symbol=symbol, TxnDate=current_date,
                          TxnPrice=enter_price, TxnQty=qty, TxnFees=0)
        }
      }
    } # End asset loop
  } # End dates loop
  # Final update after last transactions (last day only)
  updatePortf(Portfolio=portfolioName, Dates=ind[length(ind)])
  a <- list(getPortfolio(portfolioName))
  names(a) <- portfolioName
  return(a)
}

#' Similar to `place_transactions_index` but transacts on the last
#' traded price/membership date.
#' @importFrom zoo index
place_transactions_ordering <- function(initEq,
                                        assets, Currency, ind, start_i=1, K_m=1, wts=wts,
                                        initdate=initdate, enddate=enddate,
                                        txn_data=NULL, enter_pefer="Close",
                                        exit_prefer="Close"){
  FinancialInstrument::currency(Currency)
  FinancialInstrument::stock(assets, currency = Currency)
  #--- Create flag every K months to buy/sell
  p <- which(rowSums(abs(wts)) != 0)[1]
  month_index <- xts(matrix(c(rep(0,p-1),K_m:(nrow(wts)+K_m-p)),ncol=1),
                     order.by=index(wts))
  wt_ind <- month_index - start_i + 1
  wt_ind <- wt_ind[(wt_ind %% K_m == 0) & (wt_ind > 0),]
  wts_i <- wts[index(wt_ind),]
  portfolioName <- paste0("portfolio_",start_i)
  suppressWarnings(rm(list=c(paste0("portfolio.",portfolioName)), pos=.blotter))
  blotter::initPortf(name=portfolioName, symbols=assets, initDate=initdate)
  for( i in 1:length(ind) ) { # Start date loop
    current_date <- ind[i]
    drange <- paste0(ind[1],"/",current_date)
    # Transaction ordering
    dr <- txn_data[txn_data$Date==as.Date(current_date),
                   setdiff(names(txn_data),"Date")]
    txn_order <- names(sort(rank(dr,ties.method = "first")))
    # Check all assets are still members
    if (!(current_date %in% index(wt_ind))){
      for (j in 1:length(assets)){
        symbol <- txn_order[j]
        Posn <- blotter::getPosQty(portfolioName, Symbol = symbol,
                          Date = current_date)
        if (Posn !=0){
          if(is.na(get(symbol)[current_date,"Member"]) |
              (get(symbol)[current_date,"Member"] !=1)){
            last_day <- dr[,symbol]
            sym <- get(symbol)[paste0("::",current_date)]
            price <- sym[last_day,exit_prefer]# last available price
            blotter::addTxn(portfolioName, Symbol=symbol, TxnDate=last_day,
                            TxnPrice=price, TxnQty=-Posn, TxnFees=0)
          }
        }
      }
    }
    blotter::updatePortf(updatePortf(Portfolio=portfolioName, Dates=drange))
    if(current_date %in% index(wt_ind)){ # if entry date
      equity<-sum(blotter::dailyStats(portfolioName,use="equity")[1])+(initEq)
      for (j in 1:length(assets)){
        symbol <- txn_order[j]
        sym <- get(symbol)
        last_day <- dr[,symbol]
        enter_price <- sym[last_day,enter_prefer]
        exit_price <- sym[last_day,exit_prefer]
        Posn <- blotter::getPosQty(portfolioName, Symbol = symbol,
                          Date = current_date)
        entrySig <- wts[current_date,symbol]
        if (Posn != 0){ # If holding position, sell
          if(is.na(exit_price)){
            exit_price <- sym[last(which(!is.na(sym[,exit_prefer]))),exit_prefer]# last available price
            last_day <- index(sym[last(which(!is.na(sym[,exit_prefer]))),exit_prefer])
          }
          blotter::addTxn(portfolioName, Symbol=symbol, TxnDate=last_day,
                          TxnPrice=exit_price, TxnQty=-Posn, TxnFees=0)
        }
        if ((entrySig != 0) & holdingTime(current_date,as.Date(enddate),K_m) &
            (equity > 0)){ # 2nd cond to align with bruce
          if(is.na(enter_price)){
            # last available price
            enter_price <- sym[last(which(!is.na(sym[,enter_prefer]))),
                               enter_prefer]
            last_day <- index(sym[last(which(!is.na(sym[,exit_prefer]))),
                                  enter_prefer])
          }
          qty <- as.numeric(trunc((abs(equity)*
                                     wts[current_date,symbol])/enter_price))
          blotter::addTxn(portfolioName, Symbol=symbol, TxnDate=last_day,
                          TxnPrice=enter_price, TxnQty=qty, TxnFees=0)
        }
      }
    } # End asset loop
  } # End dates loop
  # Final update after last transactions (last day only)
  blotter::updatePortf(Portfolio=portfolioName, Dates=ind[length(ind)])
  a <- list(blotter::getPortfolio(portfolioName))
  names(a) <- portfolioName
  return(a)
}

#' Function to perform rank by an external (pre-calculated) variable, accounting
#'  for membership.
#'  @description Function to perform rank by an external (pre-calculated)
#'  variable, accounting for membership. Analogous to `momRankMembers` but
#'  assumes a pre-calculated variable, instead of calculating and ranking on
#'  price momentum.
#'  @param  dn xts object of variables ready to rank. Columns assumed to be
#'  individual assets.
#' @param s Skip period (integer). This is the time between the variable
#' calculation date (end of formation period) and ranking
#' (and presumably investment) date.
#' @param hist_members xts object containing membership data. Column names
#' are assumed to be asset names corresponding to those in df. Note: All entries
#' not NA are assumed to represent active membership.
#' @importFrom zoo index
VarRankMembers <- function(dn, s=0, hist_members = newHM){
  # dn is xts of variables (such as news). Cols are stocks.
  rmat <- as.xts(matrix(0,ncol=ncol(dn),nrow=nrow(dn)),order.by=index(dn))
  names(rmat) <- names(dn)
  hist_members <- hist_members[, base::intersect(names(hist_members),names(dn))]
  for (i in 1:nrow(dn)){
    idate <- as.Date(index(dn)[i])
    members <- names(hist_members)[which(!is.na(hist_members[idate,]))]
    members_with_data <- members[!is.na(as.numeric(dn[i-s,members]))]
    if(length(members_with_data)==0){next()}
    rmat[i,members_with_data] <- rank(-as.numeric(dn[i-s,members_with_data]),
                                    ties.method = "first")
  }
  return(rmat)
}

multiRank <- function(Vars,groupings){
  # Function sorts members into groupings for number of variables.
  # Vars should be a list of xts objects of ranked stocks where <=0 means not a member
  # i.e. output from momRankMembers. Groupings should be an array of the number of
  # groups to create within each variable.
  # Example: ranks3 <- momRankMembers(DF,n=3);ranks6 <- momRankMembers(DF,n=6)
  # out <- multiRank(Vars=list(mom_6m=ranks_6m,mom_3m=ranks_3m), groupings = c(5,2))
  # out[[2]] is xts of
  M_list <- list()
  for (i in 1:length(groupings)){
    df <- as.matrix(Vars[[i]])
    vmat <- as.xts(matrix(0,nrow=nrow(df),ncol=ncol(df)),order.by=index(Vars[[i]]))
    names(vmat) <- colnames(df)
    if(i > 1){
      vlast <- M_list[[i-1]]
      for (j in 1:nrow(df)){
        jdate <- index(vmat)[j]
        for (k in 1:groupings[i-1]){
          groupMembers <- names(vlast)[vlast[jdate,]==k]
          if(length(groupMembers)<groupings[i]){
            warning(paste0("The number of assets within group ",k,
                           " of variable ",i-1," data is less than the number",
                           "of desired ","groups for variable ",i,": ",jdate))
            next()}
          groupMembers <- groupMembers[df[j,groupMembers] > 0]
          if (length(groupMembers) < groupings[i]){
            warning(paste0("The number of assets to be ranked that have ",
                           "variable ",i," data is less than the number of ",
                           "desired groups: ",jdate))
          }
          # breaks <- seq.int(0,length(groupMembers),trunc(length(groupMembers)/groupings[i]))
          # breaks[length(breaks)-1]<-breaks[length(breaks)-1]+length(groupMembers)-breaks[length(breaks)]
          # # The above line puts extra (non divisible) assets in second last bin. Since first and last are usually more important
          # breaks[1] <-1; breaks[length(breaks)] <- length(groupMembers)
          breaks <- trunc(seq.int(1, length(groupMembers),
                                  length.out=groupings[i]+1))
          if (length(groupMembers)==groupings[i]){
            group_allocation <- as.numeric(rank(df[j, groupMembers]))
          } else {
            group_allocation <-as.numeric(
              cut(rank(df[j,groupMembers]), breaks = breaks, right = TRUE,
                  labels = as.character(1:groupings[i]),include.lowest = TRUE))
          }
          vmat[j,groupMembers] <- group_allocation
        }
      }
      M_list[[i]] <- vmat
    } else{
      for (j in 1:nrow(df)){
        members <- colnames(df)[which(df[j,] > 0)]
        if(length(members)<groupings[i]){
          warning(paste0("The number of assets to be ranked that have variable "
                         ,i," data is less than the number of desired groups: ",
                         index(Vars[[i]])[j]))
          next()}
        # breaks <- seq.int(0,length(members),trunc(length(members)/groupings[i]))
        # breaks[length(breaks)-1]<-breaks[length(breaks)-1]+(length(members)-breaks[length(breaks)])
        # breaks[1] <-1; breaks[length(breaks)] <- length(members)
        breaks <- trunc(seq(1,length(members),length.out = groupings[i]+1))
        group_allocation <-as.numeric(
          cut(df[j,members],breaks = breaks,right = TRUE,
              labels = as.character(1:groupings[i]),include.lowest = TRUE))
        vmat[j,members] <- group_allocation
      }
      M_list[[i]] <- vmat
    }
  }
  if (is.null(names(Vars))){
    names(M_list) <- paste0("group_",1:length(groupings))
  } else{
    names(M_list) <- names(Vars)
  }
  return(M_list)
}


#' Need to double check!!
#' @note Much of the apparant complexity/ugliness in this fuction is to deal
#' with the case in which there is no price data in the current month. We could
#' just return an NA in all these cases, but there is the special case in which
#' this is not ideal: If the last day with membership and/or price data falls on
#' the end point of the previous period. Because positions are often evaluated
#' and entered based on end-of-period points, there is the possibility that we
#' could have entered such a position, and then have no reference point for
#' exit price or date the next period when we realise there is no longer
#'  membership or data.
txn_dates2 <- function(df, membership=TRUE, hist_members=newHM,
                       on="months", ind=NULL){
  if(is.null(ind)){
    eps <- xts::endpoints(df,on=on)
    eps[1] <- 1 # New added! If it breaks, try without this!
    ind <- index(df)[eps]
  } else {
    ind <- ind
  }
  dym <- as.data.frame(matrix(NA,nrow=length(ind),ncol=ncol(df)))
  names(dym) <- names(df)
  dym$Date <- ind
  if (membership){
    # Ensure asset names in membership only contain those in df
    hist_members <- hist_members[,
                                 base::intersect(names(hist_members),names(df))]
    for(j in 1:ncol(df)){
      sym <- names(df)[j]
      last_dat <- which(!(is.na(df[ind,sym])))
      last_dat <- min(last_dat[length(last_dat)]+1, length(ind))
      for(i in 1:last_dat){
        if (i==1){
          drange <- paste0(index(df)[1],"/",ind[i])
        } else {drange <- paste0(ind[i-1]+days(1),"/",ind[i])}
        df_sub <- df[drange,sym]
        mem_sub <- hist_members[drange,sym]
        with_data <- as.Date(index(df_sub)[which(!is.na(df_sub[,sym]))])
        with_membership <- as.Date(index(mem_sub)[which(!is.na(mem_sub[,sym]))])
        commn <- intersect(with_data,with_membership)
        if (length(commn)==0){
          # if no longer a member, use last trading data
          commn <- with_data
          # if no trading data either, use last trading day of previous month
          if(length(with_data)==0){
            if (i > 1){
              if (!is.na(dym[i-1,sym])){
                # Catch instances when last data falls on last day of previous month
                if (as.Date(dym[i-1,sym])==as.Date(ind[i-1])){
                  commn <- dym[i-1,sym]
                } else {next()}
              } else{next()}
            } else {next()}
          }
        }
        dym[i,sym] <- max(commn)
      }
    }
    for (i in 1:ncol(dym)){
      dym[,i] <- as.Date(dym[,i])
    }
    return(dym)
  } else{
    for(j in 1:ncol(df)){
      sym <- names(df)[j]
      last_dat <- which(!(is.na(df[ind,sym])))
      last_dat <- min(last_dat[length(last_dat)]+1, length(ind))
      for(i in 1:last_dat){
        if (i==1){
          drange <- paste0(index(df)[1],"/",ind[i])
        } else {drange <- paste0(ind[i-1]+days(1),"/",ind[i])}
        df_sub <- df[drange,sym]
        with_data <- as.Date(index(df_sub)[which(!is.na(df_sub[,sym]))])
        if(length(with_data)==0){
          if (i > 1){
            if (!is.na(dym[i-1,sym])){
              if (as.Date(dym[i-1,sym])==as.Date(ind[i-1])){
                commn <- dym[i-1,sym]
              } else {(next())}
            } else {next()}
          } else {next()}
        }
        dym[i,sym] <- max(commn)
      }
    }
    for (i in 1:ncol(dym)){
      dym[,i] <- as.Date(dym[,i])
    }
    return(dym)
  }
}

#' Function to get equal-weighted (zero-cost) long-short wts from selected
#' groupings of multi-ranked variables.
#' @note For example, if one wanted to long high momentum stocks with positive
#' news and short negative momentum stocks with negative news, one could sort
#' the top (bottom) quintile (1/5) of momentum stocks on news tone and long
#' (short) the top (bottom) tercile (1/3). Weights for this could be calculated
#'  as follows:
#' mr <- multiRank(Vars=list(momentum_ranks, news_ranks), groupings = c(5,3))
#' wts <- get_weights(mr, long_g=c(1,1), short_g=c(5,3))
get_weights_mr <- function(mranks, long_g, short_g, long_only=FALSE){
  if(long_only){
    if (length(mranks) != length(long_g)){
      stop('Length of long_g must equal the number of variables in
         mranks.')
    }
    v1 <- mranks[[1]]
    lo <- v1[index(v1), ] == long_g[1]
    for (i in 2:length(mranks)){
      vi <- mranks[[i]]
      li <- lo & (vi[index(vi)] == long_g[i])
      lo <- li
    }
    li[index(li), ] <- as.numeric(li)
    # Equal weight
    n_l <- rowSums(li)
    wts <- na.fill(li/n_l, 0)
    return(wts)
  } else {
    if ((length(mranks) != length(long_g))|(length(long_g) != length(short_g))){
      stop('Length of long_g and short_g must equal the number of variables in
         mranks.')
    }
    v1 <- mranks[[1]]
    lo <- v1[index(v1), ] == long_g[1]
    so <- v1[index(v1), ] == short_g[1]
    for (i in 2:length(mranks)){
      vi <- mranks[[i]]
      li <- lo & (vi[index(vi)] == long_g[i])
      lo <- li
      si <- so & (vi[index(vi)] == short_g[i])
      so <- si
    }
    li[index(li), ] <- as.numeric(li)
    si[index(si), ] <- as.numeric(si)
    # Equal weight long and short legs
    n_l <- rowSums(li)
    n_s <- rowSums(si)
    li <- na.fill(li/n_l, 0)
    si <- na.fill(si/n_s, 0)
    # Long short portfolio weights
    wts <- li-si
    return(wts)
  }
}
