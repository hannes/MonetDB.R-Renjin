library(hamcrest)
library(MonetDB.R)

test.driver <- function() {
	con <- dbConnect(MonetDB.R(), "jdbc:monetdb://localhost:50000/mTests_clients_R", 
			"monetdb", "monetdb")
	res <- dbGetQuery(con, "SELECT 1")

	assertThat(res, instanceOf("data.frame"))
	assertThat(dim(res), equalTo(c(1, 1)))
}

# "/Users/hannes/source/MonetDB/java/monetdb-jdbc-2.12.jar"
