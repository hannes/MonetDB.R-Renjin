# JDBC DBI for Renjin, <hannes@cwi.nl>

JDBC <- function(drvname, ...) {
	# this will fail if the driver is not in the classpath
	# FIXME import(drvname)
	structure(list(drvname = drvname, ...), class = c("JDBCDriver", "DBIDriver"))
}
# TODO: provide same API as the MonetDB.R package...
MonetDB.R <- MonetDB <- function() {
	import(nl.cwi.monetdb.jdbc.MonetDriver)
	structure(list(), class = c("MonetDBDriver", "JDBCDriver", "DBIDriver"))
} 

# some DBI methods in S3 form for source compatibility
# driver
dbDriver          <- function(drvName, ...)   do.call(drvName, list(...))

dbConnect         <- function(drv, ...)       UseMethod("dbConnect") 
dbIsValid         <- function(obj, ...)       UseMethod("dbIsValid") 
dbGetInfo         <- function(obj, ...)       UseMethod("dbGetInfo") 

# connection
dbDisconnect      <- function(con, ...)       UseMethod("dbDisconnect") 
dbListTables      <- function(con, ...)       UseMethod("dbListTables") 
dbBegin           <- function(con, ...)       UseMethod("dbBegin") 
dbCommit          <- function(con, ...)       UseMethod("dbCommit") 
dbRollback        <- function(con, ...)       UseMethod("dbRollback")
dbListFields      <- function(con, name, ...) UseMethod("dbListFields")
dbExistsTable     <- function(con, name, ...) UseMethod("dbExistsTable")
dbReadTable       <- function(con, name, ...) UseMethod("dbReadTable")
dbRemoveTable     <- function(con, name, ...) UseMethod("dbRemoveTable")
dbSendQuery       <- function(con, qry, ...)  UseMethod("dbSendQuery")
dbSendUpdate      <- function(con, qry, ...)  UseMethod("dbSendUpdate")
dbGetQuery        <- function(con, qry, ...)  UseMethod("dbGetQuery")
dbQuoteIdentifier <- function(con, idnt, ...) UseMethod("dbQuoteIdentifier")
dbDataType        <- function(con, obj, ...)  UseMethod("dbDataType")
dbGetException    <- function(con, ...)       UseMethod("dbGetException")

# result sets
fetch <- dbFetch  <- function(res, n, ...)    UseMethod("dbFetch")
dbHasCompleted    <- function(res, ...)       UseMethod("dbHasCompleted")
dbClearResult     <- function(res, ...)       UseMethod("dbClearResult")
dbColumnInfo      <- function(res, ...)       UseMethod("dbColumnInfo")

dbQuoteString     <- function(con, x, ...)    UseMethod("dbQuoteString")
dbQuoteIdentifier <- function(con, x, ...)    UseMethod("dbQuoteIdentifier")


dbQuoteString.JDBCConnection <- function(con, x, ...) {
	if (substring(x, 1, 2) == "'") {
		return(x)
	}
	x <- gsub("'", "''", x, fixed = TRUE)
	paste("'", x, "'", sep = "")
}

dbQuoteIdentifier.JDBCConnection <- function(con, x, ...) {
	if (substring(x, 1, 2) == '"') {
		return(x)
	}
	x <- gsub('"', '""', x, fixed = TRUE)
	paste('"', x, '"', sep = "")
}

# custom stuff
dbWriteTable      <- function(conn, name, value, ...) UseMethod("dbWriteTable")

dbIsValid.JDBCDriver <- function(drv) {
	inherits(drv, "JDBCDriver")
}

dbGetInfo.JDBCDriver <- function(obj, ...) {
	list()
}

# TODO: how can we check this? SELECT 1?
dbIsValid.JDBCConnection <- function(con) {
	inherits(con, "JDBCConnection")
}

dbConnect.JDBCDriver <- function(drv, url, username, password) {
	# this will throw an exception if it fails, so no need for additional checks here.
	if (getOption("dbi.debug", F)) message("II: Connecting to ",url," with user ", username, " and a non-printed password.")
	jconn <- import(java.sql.DriverManager)$getConnection(url, username, password);
	structure(list(conn = jconn), class = c("JDBCConnection", "DBIConnection"))
}

# this takes the same parameters as dbConnect() parameters in MonetDB.R for compatibility
dbConnect.MonetDBDriver <- function(drv, dbname="demo", user="monetdb", 
		password="monetdb", host="localhost", port=50000L, timeout=86400L, wait=FALSE, language="sql", 
		..., url="") {
	if (substring(dbname, 1, 5) != "jdbc:") {
		if (substring(dbname, 1, 10) == "monetdb://") {
			url <- paste("jdbc:", dbname, sep="")
		} else {
			url <- paste("jdbc:monetdb://", host,":", as.integer(port), "/", dbname, sep="")
		}
	} else {
		url <- dbname
	}
	# TODO: auto-assign a specific class to the connection based on the driver name for ez overloading
	structure(list(conn = dbConnect.JDBCDriver(drv, url, user, password)$conn), class = c("MonetDBConnection", "JDBCConnection"))
}

dbSendQuery.JDBCConnection <- function (con, qry) {
	if (getOption("dbi.debug", F))  message("QQ: '", qry, "'")
	
	stmt <- con$conn$createStatement()
	res <-  stmt$execute(qry)
	structure(list(query = qry, statement = stmt, resultset = stmt$resultSet, success = TRUE), 
			class = "JDBCResultSet")
}

dbSendUpdate.JDBCConnection <- function(con, qry, ...) {
	if(length(list(...))){
		if (length(list(...))) qry <- .bindParameters(con, qry, list(...))
	}
	res <- dbSendQuery(con, qry)
	if (!res$success) {
		stop(qry, " failed!\nServer says:", "TODO")
	}
	invisible(TRUE)
}

dbReadTable.JDBCConnection <- function(con, name, ...) {
	if (!dbExistsTable(con, name))
		stop(paste0("Unknown table: ", name));
	dbGetQuery(con,paste0("SELECT * FROM ", name))
}

# copied from DBI
dbGetQuery.JDBCConnection <- function(con, qry, ...) {
	rs <- dbSendQuery(con, qry, ...)
	on.exit(dbClearResult(rs))
	if (dbHasCompleted(rs)) return(NULL)
	res <- dbFetch(rs, n = -1, ...)
	if (!dbHasCompleted(rs)) warning("pending rows")
	res
}

dbExistsTable.JDBCConnection <- function(con, name, ...) {
	tolower(gsub("(^\"|\"$)","",as.character(name))) %in% tolower(dbListTables(con,sys_tables=T))
}


dbBegin.JDBCConnection <- function(con, ...) {
	con$conn$autoCommit <- FALSE
	invisible(TRUE)
} 

dbCommit.JDBCConnection <- function(con, ...) {
	con$conn$commit()
	con$conn$autoCommit <- TRUE
	invisible(TRUE)
} 

dbRollback.JDBCConnection <- function(con, ...) {
	con$conn$rollback()
	con$conn$autoCommit <- TRUE
	invisible(TRUE)
} 

dbDisconnect.JDBCConnection <- function(con, ...) {
	TRUE
}

# this is db-specific, also copied from MonetDB.R
dbListTables.MonetDBConnection <- function(con, ..., sys_tables=F, schema_names=F, quote=F) {
	q <- "select schemas.name as sn, tables.name as tn from sys.tables join sys.schemas on tables.schema_id=schemas.id"
	if (!sys_tables) q <- paste0(q, " where tables.system=false")
	df <- dbGetQuery(con, q)
	if (quote) {
		df$tn <- paste0("\"", df$tn, "\"")
	}
	res <- df$tn
	if (schema_names) {
		if (quote) {
			df$sn <- paste0("\"", df$sn, "\"")
		}
		res <- paste0(df$sn, ".", df$tn)
	}
	return(as.character(res))
}

dbListFields.MonetDBConnection <- function(con, name, ...) {
	if (!dbExistsTable(con, name))
		stop("Unknown table ", name);
	df <- dbGetQuery(con, paste0("select columns.name as name from sys.columns join sys.tables on columns.table_id=tables.id where tables.name='", name, "';"))	
	df$name
}

dbRemoveTable.MonetDBConnection <- function(con, name, ...) {
	if (dbExistsTable(con, name)) {
		dbSendUpdate(con, paste("DROP TABLE", tolower(name)))
		return(invisible(TRUE))
	}
	return(invisible(FALSE))
}

dbDataType.MonetDBConnection <- function(con, obj, ...) {
	if (is.logical(obj)) "BOOLEAN"
	else if (is.integer(obj)) "INTEGER"
	else if (is.numeric(obj)) "DOUBLE PRECISION"
	else if (is.raw(obj)) "BLOB"
	else "STRING"
}

# dealing with ResultSet and ResultSetMetaData objects is far too ugly to do here
dbFetch.JDBCResultSet <- function(res, n, ...) {
	import(org.renjin.cran.MonetDBR.ResultSetConverter)$fetch(res$resultset, n)	
}

dbColumnInfo.JDBCResultSet <- function(res, ...) {
	cinf <- import(org.renjin.cran.MonetDBR.ResultSetConverter)$columnInfo(res$resultset)	
	tpes <- unlist(lapply(cinf,'[[',"type"))
	nmes <- unlist(lapply(cinf,'[[',"name"))
	data.frame(field.name=nmes, field.type=tpes, data.type=.monetTypes[tpes])
}

dbHasCompleted.JDBCResultSet <- function(res, ...) {
	# FIXME this breaks, falling back to Java function, but it really is simple enough, so it should be here
	#res$resultset$afterLast || res$resultset$closed
	import(org.renjin.cran.MonetDBR.ResultSetConverter)$hasCompleted(res$resultset)
}

dbClearResult.JDBCResultSet <- function(res, ...) {
	res$resultset$close()
	res$statement$close()
}

# TODO: how to check this? do we need an env after all?
dbIsValid.JDBCResultSet <- function(res, ...) {
	TRUE
}

dbWriteTable.MonetDBConnection <- function(conn, name, value, overwrite=FALSE, 
		append=FALSE, csvdump=FALSE, transaction=TRUE, ...) {
	if (is.vector(value) && !is.list(value)) value <- data.frame(x=value)
	if (length(value)<1) stop("value must have at least one column")
	if (is.null(names(value))) names(value) <- paste("V", 1:length(value), sep='')
	if (length(value[[1]])>0) {
		if (!is.data.frame(value)) value <- as.data.frame(value, row.names=1:length(value[[1]]))
	} else {
		if (!is.data.frame(value)) value <- as.data.frame(value)
	}
	if (overwrite && append) {
		stop("Setting both overwrite and append to true makes no sense.")
	}
	qname <- .make.db.names(name)
	if (dbExistsTable(conn, qname)) {
		if (overwrite) dbRemoveTable(conn, qname)
		if (!overwrite && !append) stop("Table ", qname, " already exists. Set overwrite=TRUE if you want 
							to remove the existing table. Set append=TRUE if you would like to add the new data to the 
							existing table.")
	}
	if (!dbExistsTable(conn, qname)) {
		fts <- sapply(value, function(x) {
					dbDataType(conn, x)
				})
		fdef <- paste(.make.db.names(names(value)), fts, collapse=', ')
		ct <- paste("CREATE TABLE ", qname, " (", fdef, ")", sep= '')
		dbSendUpdate(conn, ct)
	}
	if (length(value[[1]])) {
		vins <- paste("(", paste(rep("?", length(value)), collapse=', '), ")", sep='')
		if (transaction) dbBegin(conn)
		# chunk some inserts together so we do not need to do a round trip for every one
		splitlen <- 0:(nrow(value)-1) %/% getOption("monetdb.insert.splitsize", 1000)
		lapply(split(value, splitlen), 
			function(valueck) {
				bvins <- c()
				for (j in 1:length(valueck[[1]])) {
					bvins <- c(bvins, .bindParameters(conn, vins, as.list(valueck[j, ])))
				} 
				dbSendUpdate(conn, paste0("INSERT INTO ", qname, " VALUES ",paste0(bvins, collapse=", ")))
			})
		if (transaction) dbCommit(conn)		
	}
	invisible(TRUE)
}

monet.read.csv <- monetdb.read.csv <- function(conn, files, tablename, nrows, header=TRUE, 
		locked=FALSE, na.strings="", nrow.check=500, delim=",", newline="\\n", quote="\"", ...){
	
	if (length(na.strings)>1) stop("na.strings must be of length 1")
	headers <- lapply(files, utils::read.csv, sep=delim, na.strings=na.strings, quote=quote, nrows=nrow.check, 
			...)
	
	if (length(files)>1){
		nn <- sapply(headers, ncol)
		if (!all(nn==nn[1])) stop("Files have different numbers of columns")
		nms <- sapply(headers, names)
		if(!all(nms==nms[, 1])) stop("Files have different variable names")
		types <- sapply(headers, function(df) sapply(df, dbDataType, dbObj=conn))
		if(!all(types==types[, 1])) stop("Files have different variable types")
	} 
	
	dbWriteTable(conn, tablename, headers[[1]][FALSE, ])
	
	delimspec <- paste0("USING DELIMITERS '", delim, "','", newline, "','", quote, "'")
	
	if(header || !missing(nrows)){
		if (length(nrows)==1) nrows <- rep(nrows, length(files))
		for(i in seq_along(files)) {
			thefile <- normalizePath(files[i])
			dbSendUpdate(conn, paste("COPY", format(nrows[i], scientific=FALSE), "OFFSET 2 RECORDS INTO", 
							tablename, "FROM", paste("'", sub("file://", "", thefile, fixed=T), "'", sep=""), delimspec, "NULL as", paste("'", 
									na.strings[1], "'", sep=""), if(locked) "LOCKED"))
		}
	} else {
		for(i in seq_along(files)) {
			thefile <- normalizePath(files[i])
			dbSendUpdate(conn, paste0("COPY INTO ", tablename, " FROM ", paste("'", sub("file://", "", thefile, fixed=T), "'", sep=""), 
							delimspec, "NULL as ", paste("'", na.strings[1], "'", sep=""), if(locked) " LOCKED "))
		}
	}
	dbGetQuery(conn, paste("select count(*) from", tablename))
}


.bindParameters <- function(con, statement, param) {
	for (i in 1:length(param)) {
		value <- param[[i]]
		valueClass <- class(value)
		if (is.na(value)) 
			statement <- sub("?", "NULL", statement, fixed=TRUE)
		else if (valueClass %in% c("numeric", "logical", "integer"))
			statement <- sub("?", value, statement, fixed=TRUE)
		else if (valueClass == c("raw"))
			stop("raw() data is so far only supported when reading from BLOBs")
		else
			statement <- sub("?", paste("'", dbQuoteString(con, toString(value)), "'", sep=""), statement, 
					fixed=TRUE)
	}
	statement
}

.monetTypes <- rep(c("numeric", "character", "character", "logical", "raw"), c(9, 3, 4, 1, 1))
names(.monetTypes) <- c(c("TINYINT", "SMALLINT", "INT", "BIGINT", "HUGEINT", "REAL", "DOUBLE", "DECIMAL", "WRD"), 
		c("CHAR", "VARCHAR", "CLOB"), 
		c("INTERVAL", "DATE", "TIME", "TIMESTAMP"), 
		"BOOLEAN", 
		"BLOB")

# copied from DBI
.SQL92Keywords <- c("ABSOLUTE", "ADD", "ALL", "ALLOCATE", "ALTER", "AND", "ANY", 
		"ARE", "AS", "ASC", "ASSERTION", "AT", "AUTHORIZATION", "AVG", "BEGIN", 
		"BETWEEN", "BIT", "BIT_LENGTH", "BY", "CASCADE", "CASCADED", "CASE", "CAST",
		"CATALOG", "CHAR", "CHARACTER", "CHARACTER_LENGTH", "CHAR_LENGTH",
		"CHECK", "CLOSE", "COALESCE", "COLLATE", "COLLATION", "COLUMN",
		"COMMIT", "CONNECT", "CONNECTION", "CONSTRAINT", "CONSTRAINTS",
		"CONTINUE", "CONVERT", "CORRESPONDING", "COUNT", "CREATE", "CURRENT",
		"CURRENT_DATE", "CURRENT_TIMESTAMP", "CURRENT_TYPE", "CURSOR", "DATE",
		"DAY", "DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT",
		"DEFERRABLE", "DEFERRED", "DELETE", "DESC", "DESCRIBE", "DESCRIPTOR",
		"DIAGNOSTICS", "DICONNECT", "DICTIONATRY", "DISPLACEMENT", "DISTINCT",
		"DOMAIN", "DOUBLE", "DROP", "ELSE", "END", "END-EXEC", "ESCAPE",
		"EXCEPT", "EXCEPTION", "EXEC", "EXECUTE", "EXISTS", "EXTERNAL",
		"EXTRACT", "FALSE", "FETCH", "FIRST", "FLOAT", "FOR", "FOREIGN",
		"FOUND", "FROM", "FULL", "GET", "GLOBAL", "GO", "GOTO", "GRANT",
		"GROUP", "HAVING", "HOUR", "IDENTITY", "IGNORE", "IMMEDIATE", "IN",
		"INCLUDE", "INDEX", "INDICATOR", "INITIALLY", "INNER", "INPUT",
		"INSENSITIVE", "INSERT", "INT", "INTEGER", "INTERSECT", "INTERVAL",
		"INTO", "IS", "ISOLATION", "JOIN", "KEY", "LANGUAGE", "LAST", "LEFT",
		"LEVEL", "LIKE", "LOCAL", "LOWER", "MATCH", "MAX", "MIN", "MINUTE",
		"MODULE", "MONTH", "NAMES", "NATIONAL", "NCHAR", "NEXT", "NOT", "NULL",
		"NULLIF", "NUMERIC", "OCTECT_LENGTH", "OF", "OFF", "ONLY", "OPEN",
		"OPTION", "OR", "ORDER", "OUTER", "OUTPUT", "OVERLAPS", "PARTIAL",
		"POSITION", "PRECISION", "PREPARE", "PRESERVE", "PRIMARY", "PRIOR",
		"PRIVILEGES", "PROCEDURE", "PUBLIC", "READ", "REAL", "REFERENCES",
		"RESTRICT", "REVOKE", "RIGHT", "ROLLBACK", "ROWS", "SCHEMA", "SCROLL",
		"SECOND", "SECTION", "SELECT", "SET", "SIZE", "SMALLINT", "SOME", "SQL",
		"SQLCA", "SQLCODE", "SQLERROR", "SQLSTATE", "SQLWARNING", "SUBSTRING",
		"SUM", "SYSTEM", "TABLE", "TEMPORARY", "THEN", "TIME", "TIMESTAMP",
		"TIMEZONE_HOUR", "TIMEZONE_MINUTE", "TO", "TRANSACTION", "TRANSLATE",
		"TRANSLATION", "TRUE", "UNION", "UNIQUE", "UNKNOWN", "UPDATE", "UPPER",
		"USAGE", "USER", "USING", "VALUE", "VALUES", "VARCHAR", "VARYING",
		"VIEW", "WHEN", "WHENEVER", "WHERE", "WITH", "WORK", "WRITE", "YEAR",
		"ZONE")

.make.db.names <- function(snames, keywords = .SQL92Keywords, 
		unique = TRUE, allow.keywords = TRUE) {
	makeUnique <- function(x, sep = "_") {
		if(length(x)==0) return(x)
		out <- x
		lc <- make.names(tolower(x), unique=FALSE)
		i <- duplicated(lc)
		lc <- make.names(lc, unique = TRUE)
		#if (length(which(i)) > 0) # TODO: check why this is no longer neccessary
		out[i] <- paste(out[i], substring(lc[i], first=nchar(out[i])+1), sep=sep)
		out
	}
	## Note: SQL identifiers *can* be enclosed in double or single quotes
	## when they are equal to reserverd keywords.
	fc <- substring(snames, 1, 1)
	lc <- substring(snames, nchar(snames))
	i <- match(fc, c("'", '"'), 0)>0 & match(lc, c("'", '"'), 0)>0
	snames[!i]  <- make.names(snames[!i], unique=FALSE)	
	
	if(unique)
		snames[!i] <- makeUnique(snames[!i])
	
	if(!allow.keywords){
		kwi <- match(keywords, toupper(snames), nomatch = 0L)
		snames[kwi] <- paste('"', snames[kwi], '"', sep='')
	}
	
	gsub("\\.", "_", snames)
}
