library(survey)

test.driver <- function() {
	svydsgn <- svrepdesign(
		weight = ~pwgtp ,
		repweights = 'pwgtp[1-9]' ,
		scale = 4 / 80 ,
		rscales = rep( 1 , 80 ) ,
		mse = TRUE ,
		data = readRDS("alabama.rds"))

	return(list(svymean(~agep, svydsgn, se=TRUE),
		svytotal(~I(relp %in% 0:15), svydsgn),
		svytotal(~I(relp %in% 0:15), svydsgn),
		svytotal(~I(relp %in% 16:17), svydsgn),
		svytotal(~I(relp == 16), svydsgn),
		svytotal(~I(relp == 17), svydsgn),
		svytotal(~sex, svydsgn),
		svytotal(~I(agep %in% 0:4) + I(agep %in% 5:9) + I(agep %in% 10:14) + I(agep %in% 15:19), svydsgn),
		svytotal(~I(agep %in% 20:24) + I(agep %in% 25:34) + I(agep %in% 35:44) + I(agep %in% 45:54), svydsgn),
		svytotal(~I(agep %in% 55:59) + I(agep %in% 60:64) + I(agep %in% 65:74) + I(agep %in% 75:84) + I(agep > 84), svydsgn)
	))
}
