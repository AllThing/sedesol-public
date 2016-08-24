#' Scale to 0-1 range
range01 <- function(x) {
    (x-min(x)) / (max(x)-min(x))
}

#' Get the kernel similarity to an LBM score
get_lbm_score <- function(x, mu = 1500, sigma = 10) {
    z <- exp(-.5 * (x - mu) ^ 2 / (sigma ^ 2))
    z / max(z)
}

#' Reweights rows according to weight vecotr
#'
#' This reweights rows of a matrix X according to the linear combination X * w
#'
#' @param matrix X A matrix whose rows we want to reorder
#' @param vector w A vector of weights to assign to each column of X
#' @return vector An ordering of the rows of X, attempting to maximize the
#' linear combination X * w
row_scores <- function(X, w) {
    w <- w / sum(w)
    X %*% w
}
