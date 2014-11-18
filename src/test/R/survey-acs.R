library(survey)

system.time(
	adata <- readRDS("alabama.rds")
)
#print(str(adata))
system.time(
	alabama <- svrepdesign(
		weight = ~pwgtp ,
		repweights = 'pwgtp[1-9]' ,
		scale = 4 / 80 ,
		rscales = rep( 1 , 80 ) ,
		mse = TRUE ,
		data = adata)
)
system.time(
	print(svymean(~agep, alabama, se=TRUE))
)
