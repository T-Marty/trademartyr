% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/position_prep.R
\name{VarRankMembers}
\alias{VarRankMembers}
\title{Function to perform rank by an external (pre-calculated) variable, accounting
for membership.}
\usage{
VarRankMembers(dn, s = 0, hist_members = newHM)
}
\arguments{
\item{dn}{xts object of variables ready to rank. Columns assumed to be
individual assets.}

\item{s}{Skip period (integer). This is the time between the variable
calculation date (end of formation period) and ranking
(and presumably investment) date.}

\item{hist_members}{xts object containing membership data. Column names
are assumed to be asset names corresponding to those in df. Note: All entries
not NA are assumed to represent active membership.}
}
\description{
Function to perform rank by an external (pre-calculated)
variable, accounting for membership. Analogous to `momRankMembers` but
assumes a pre-calculated variable, instead of calculating and ranking on
price momentum.
}
