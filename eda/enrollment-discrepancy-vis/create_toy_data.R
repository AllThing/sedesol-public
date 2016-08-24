library("data.table")
library("dplyr")
n <- 6000
J <- 5

p_discrepancy <- data.table(
    home_id = 1:n,
    estimated_income = exp(rnorm(n)),
    self_reported_income = exp(rnorm(n)),
    prob_underreporting = runif(n),
    locality_type = c(rep("rural", n / 2),
                      rep("urban", n / 2))
)

write.csv(p_discrepancy, "data/p_discrepancy.csv", row.names=F)

