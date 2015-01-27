library(hamcrest)
library(MonetDB.R) # yeah
source("../resources/testdata.R")

test.driver <- function() {

	# force multiple INSERTs for dbWriteTable
options(monetdb.insert.splitsize = 10)
options(dbi.debug = TRUE)

	
dbname <- "mTests_clients_R"
dbport <- 50000
tname <- "monetdbtest"
drv <- dbDriver("MonetDB")
drv <- MonetDB.R()

con <- conn <- dbConnect(drv, port=dbport, dbname=dbname, wait=T)

# basic MAPI/SQL test
stopifnot(identical(dbGetQuery(con,"SELECT 'DPFKG!'")[[1]],"DPFKG!"))

# is valid?
stopifnot(dbIsValid(con))
stopifnot(dbIsValid(drv))

# remove test table
if (dbExistsTable(con,tname)) dbRemoveTable(con,tname)
stopifnot(identical(dbExistsTable(con,tname),FALSE))


# test basic handling
dbSendUpdate(con,"CREATE TABLE monetdbtest (a varchar(10),b integer,c clob)")
stopifnot(identical(dbExistsTable(con,tname),TRUE))
dbSendUpdate(con,"INSERT INTO monetdbtest VALUES ('one',1,'1111')")
dbSendUpdate(con,"INSERT INTO monetdbtest VALUES ('two',2,'22222222')")
stopifnot(identical(dbGetQuery(con,"SELECT count(*) FROM monetdbtest")[[1]],2L))
stopifnot(identical(dbReadTable(con,tname)[[3]],c("1111", "22222222")))
dbRemoveTable(con,tname)
stopifnot(identical(dbExistsTable(con,tname),FALSE))

# write test table iris
		
dbWriteTable(con,tname,iris)

stopifnot(identical(dbExistsTable(con,tname),TRUE))
stopifnot(identical(dbExistsTable(con,"monetdbtest2"),FALSE))
stopifnot(tname %in% dbListTables(con))

stopifnot(identical(dbListFields(con,tname),c("sepal_length","sepal_width",
						"petal_length","petal_width","species")))
# get stuff, first very convenient
iris2 <- dbReadTable(con,tname)
stopifnot(identical(dim(iris), dim(iris2)))

# then manually
res <- dbSendQuery(con,"SELECT species, sepal_width FROM monetdbtest")
stopifnot(dbIsValid(res))
stopifnot(identical(res$success,TRUE))

stopifnot(dbColumnInfo(res)[[1,1]] == "species")
stopifnot(dbColumnInfo(res)[[2,1]] == "sepal_width")

# cannot get row count, DBC does not export it
# stopifnot(dbGetInfo(res)$row.count == 150 && res@env$info$rows == 150)


data <- dbFetch(res, 10)

stopifnot(dim(data)[[1]] == 10)
stopifnot(dim(data)[[2]] == 2)
stopifnot(res@env$delivered == 10)
stopifnot(dbHasCompleted(res) == FALSE)

# fetch rest
data2 <- dbFetch(res, -1)
stopifnot(dim(data2)[[1]] == 140)
stopifnot(dbHasCompleted(res) == TRUE)

stopifnot(dbIsValid(res))
dbClearResult(res)
# TODO: get this to work, not now
#stopifnot(!dbIsValid(res))

# remove table again
dbRemoveTable(con,tname)
stopifnot(identical(dbExistsTable(con,tname),FALSE))

write.csv <- function(df,fl, sep=",") {
	as.csv <- function(df) {
		sapply(1:nrow(df), function(row.index)
					paste(df[row.index,], collapse=sep))
	}
	fl = file(fl)
	writeLines(paste(names(df), collapse=sep), con = fl)
	writeLines(as.csv(df), con = fl)	
	close(fl)
}


#file <- tempfile()
file <- "/tmp/dump"
# TODO: how do we do write.csv in Renjin?
write.csv(iris,file)
monetdb.read.csv(con,file,tname,150)
#unlink(file)
stopifnot(identical(dbExistsTable(con,tname),TRUE))
iris3 <- dbReadTable(con,tname)
stopifnot(identical(dim(iris),dim(iris3)))
stopifnot(identical(dbListFields(con,tname),c("sepal_length","sepal_width",
						"petal_length","petal_width","species")))
dbRemoveTable(con,tname)
stopifnot(identical(dbExistsTable(con,tname),FALSE))

# test dbWriteTable
tsize <- function(conn,tname) 
	as.integer(dbGetQuery(conn,paste0("SELECT COUNT(*) FROM ",tname))[[1]])

# clean up
if (dbExistsTable(conn,tname))
	dbRemoveTable(conn,tname)

# table does not exist, append=F, overwrite=F, this should work
dbWriteTable(conn,tname,mtcars,append=F,overwrite=F)
stopifnot(dbExistsTable(conn,tname))
stopifnot(identical(nrow(mtcars),tsize(conn,tname)))

# these should throw errors
errorThrown <- F
tryCatch(dbWriteTable(conn,tname,mtcars,append=F,overwrite=F),error=function(e){errorThrown <<- T})
stopifnot(errorThrown)

errorThrown <- F
tryCatch(dbWriteTable(conn,tname,mtcars,overwrite=T,append=T),error=function(e){errorThrown <<- T})
stopifnot(errorThrown)

# this should be fine
dbWriteTable(conn,tname,mtcars,append=F,overwrite=T)
stopifnot(dbExistsTable(conn,tname))
stopifnot(identical(nrow(mtcars),tsize(conn,tname)))

# append to existing table
dbWriteTable(conn,tname,mtcars,append=T,overwrite=F)
stopifnot(identical(as.integer(2*nrow(mtcars)),tsize(conn,tname)))
dbRemoveTable(conn,tname)

dbRemoveTable(conn,tname)
dbWriteTable(conn,tname,mtcars,append=F,overwrite=F,insert=T)
dbRemoveTable(conn,tname)

# info
#stopifnot(identical("MonetDBDriver", dbGetInfo(MonetDB.R())$name))
#stopifnot(identical("MonetDBConnection", dbGetInfo(conn)$name))

# transactions...
stopifnot(!dbExistsTable(conn,tname))
sq <- dbSendQuery(conn, "CREATE TABLE monetdbtest (a integer)")
stopifnot(dbExistsTable(conn,tname))

dbBegin(conn)
sq <- dbSendQuery(conn,"INSERT INTO monetdbtest VALUES (42)")
stopifnot(identical(1L, tsize(conn, tname)))
dbRollback(conn)

stopifnot(identical(0L, tsize(conn, tname)))
dbBegin(conn)
sq <- dbSendQuery(conn,"INSERT INTO monetdbtest VALUES (42)")
stopifnot(identical(1L, tsize(conn, tname)))
dbCommit(conn)
stopifnot(identical(1L, tsize(conn, tname)))
dbRemoveTable(conn,tname)

# funny characters in strings

# TODO: UTF escapes in JDBC, yummy. Not now. sqlsurvey does not need this after all
stopifnot(dbIsValid(conn))
#dbBegin(conn)
#sq <- dbSendQuery(conn,"CREATE TABLE monetdbtest (a string)")
#sq <- dbSendQuery(conn,"INSERT INTO monetdbtest VALUES ('Роман Mühleisen')")
#stopifnot(identical("Роман Mühleisen", dbGetQuery(conn,"SELECT a FROM monetdbtest")$a[[1]]))
#sq <- dbSendQuery(conn,"DELETE FROM monetdbtest")
#dbSendUpdate(conn, "INSERT INTO monetdbtest (a) VALUES (?)", "Роман Mühleisen")
#stopifnot(identical("Роман Mühleisen", dbGetQuery(conn,"SELECT a FROM monetdbtest")$a[[1]]))
#dbRollback(conn)

stopifnot(dbIsValid(conn))
#thrice to catch null pointer errors
stopifnot(identical(dbDisconnect(con),TRUE))
# TODO: fix this
#stopifnot(!dbIsValid(conn))
#stopifnot(identical(dbDisconnect(con),TRUE))
#stopifnot(identical(dbDisconnect(con),TRUE))

print("SUCCESS")

}
