import java.util.Properties;
import org.apache.spark.sql.SparkSession
import java.io.{PrintWriter, File}

val spark = SparkSession.
  builder().
  appName("Spark Multi DB DataPump Test").
  //config("spark.some.config.option", "some-value").
  getOrCreate()

object Connections extends Enumeration {
  type Connections = Value
  val fb, mssql = Value
}

import Connections._

def getDF(con:Connections, table:String):org.apache.spark.sql.DataFrame = {
  if (con == Connections.fb) {
    spark.read.
      format("jdbc").
      option("driver", "org.firebirdsql.jdbc.FBDriver").
      option("url", "jdbc:firebirdsql://localhost/testdb.fdb").
      option("user", "sysdba").
      option("password", "masterkey").
      option("dbtable", table).
      load()
  } else {
    spark.read.
      format("jdbc").
      option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
      option("url", "jdbc:sqlserver://localhost:1433;databaseName=test;").
      option("user", "sa").
      option("password", "mssql(!)Password").
      option("dbtable", table).
      load()
  }
}

def registerDF(con:Connections, table:String) = {
  getDF(con, table).createOrReplaceTempView(table)
}

def registerDFList(tables:Seq[(Connections, String)]) = {
  for (df <- tables) registerDF(df._1, df._2)
}

def save(name:String, content:String) = {
  val pw = new PrintWriter(new File(name))
  pw.write(content)
  pw.close()
}

registerDFList(Seq(
  (Connections.mssql, "test"),
  (Connections.mssql, "names"))
)

save("result.sql",
  spark.sql("select concat(t.id, ' -> ', n.name) as STRING from test t left join names n on n.id = t.id").
    map(row => {
      s"INSERT INTO STRINGS VALUES ('${row.getAs[String]("STRING")}');\n"
    }).collect.mkString)
