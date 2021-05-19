import java.util.Properties;
import org.apache.spark.sql.SparkSession
import java.io.{PrintWriter, File}
import org.apache.spark.sql.Row

val spark = SparkSession.
	builder().
	appName("Spark Multi DB DataPump Test").
	//config("spark.some.config.option", "some-value").
	getOrCreate()

object Connections extends Enumeration {
	type Connections = Value
	val fb, mssql, test = Value
}

import Connections._

def getDF(con:Connections, table:String):org.apache.spark.sql.DataFrame = {
	con match {
		case Connections.fb => {
			return spark.read.
				format("jdbc").
				option("driver", "org.firebirdsql.jdbc.FBDriver").
				// option("url", "jdbc:firebirdsql://192.168.1.148/grsc/Clientes/ClientesGRSA/Rockwell Automation Monterrey/Base Datos 2020/Rockwell-UPD-31Dic-2020.fdb").
				option("url", "jdbc:firebirdsql:192.168.1.148/3050:/grsc/Clientes/ClientesGRSA/Rockwell Automation Monterrey/Base Datos 2020/Rockwell-UPD-31Dic-2020.fdb").
				option("user", "sysdba").
				option("password", "masterkey").
				option("dbtable", table).
				load()
		}
		case Connections.mssql => {
			return spark.read.
				format("jdbc").
				option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
				option("url", "jdbc:sqlserver://192.168.1.151:1433;databaseName=SEERT_ROCKWELL_3_OCT26;").
				option("user", "sa").
				option("password", "mssql(!)Password").
				option("dbtable", table).
				load()
		}
		case Connections.test => {
			return spark.read.
				format("jdbc").
				option("driver", "org.firebirdsql.jdbc.FBDriver").
				// option("url", "jdbc:firebirdsql://192.168.1.148/grsc/Clientes/ClientesGRSA/Rockwell Automation Monterrey/Base Datos 2020/Rockwell-UPD-31Dic-2020.fdb").
				option("url", "jdbc:firebirdsql:localhost/3050:testdb.fdb").
				option("user", "sysdba").
				option("password", "masterkey").
				option("dbtable", table).
				load()
		}
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

def saveBatch(name:String, batchSize:Int, content:org.apache.spark.sql.Dataset[String]) = {
  for ((b, i) <- content.collect.grouped(batchSize).toList.view.zipWithIndex) {
    save(s"data/${i}_${name}", b.mkString)
  }
}


class Column(val name:String, val numeric:Boolean=false, val isKey:Boolean=false)
class SchemaTable(val name:String, val conn:Connections=null) {
	if (conn != null) {
		registerDF(conn, name)
	} 
}
class Table(val input:SchemaTable, val output:SchemaTable, val outRows:Seq[(Column, Column)], val select:String=null)

def replace(c:org.apache.spark.sql.Column):org.apache.spark.sql.Column = {
	regexp_replace(
		regexp_replace(
			regexp_replace(c, lit("\r"), lit("\\r")),
				lit("\n"),
				lit("\\n")
			),
		lit("'"),
		lit("''")
	)
}

def encapsulate(c:Column):org.apache.spark.sql.Column = {
	if (c.numeric) {
		replace(col(c.name))
	} else {
		concat(lit("'"), replace(col(c.name)), lit("'"))
	}
}

def fmtValue(c:Column):org.apache.spark.sql.Column = {
	when(col(c.name).isNull, "NULL").otherwise(
		encapsulate(c)
	)
}


def genColumn(tb:Table): org.apache.spark.sql.Column = {
	var outCol: org.apache.spark.sql.Column = concat(lit(
		s"""INSERT INTO ${tb.output.name}(${tb.outRows.map(rw=>rw._2.name).mkString(", ")})
		VALUES("""),
		fmtValue(tb.outRows.head._1)
	)
	for (c <- tb.outRows.tail.map(rw=>rw._1)) {
		outCol = concat(outCol, lit(","), fmtValue(c))
	}
	concat(outCol, lit(");\n"))
}

def getSQL(tb:Table):String = {
	if (tb.select == null) s"SELECT ${tb.outRows.map(e=>e._1.name).mkString(", ")} FROM ${tb.input.name};" else tb.select
}

def generate(tb:Table):org.apache.spark.sql.Dataset[String] = {
	spark.sql(getSQL(tb)).
		select(genColumn(tb)).
		map(_.mkString)
}

def saveBatchTable(name:String, batchSize:Int, content:Table) = {
  for ((b, i) <- generate(content).collect.grouped(batchSize).toList.view.zipWithIndex) {
    save(s"data/${i}_${name}", b.mkString)
  }
  println(s"\ndata/{n}_${name} SAVED.\n")
}
