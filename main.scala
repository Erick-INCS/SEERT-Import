import java.util.Properties;
import org.apache.spark.sql.SparkSession
import java.io.{PrintWriter, File}
import org.apache.spark.sql.Row


// val conf=SparkConf()
// conf.set("spark.executor.memory", "4g")
// conf.set("spark.cores.max", "2")
// conf.set("spark.driver.extraClassPath",
//     driver_home+'/jdbc/postgresql-9.4-1201-jdbc41.jar:'\
//     +driver_home+'/jdbc/clickhouse-jdbc-0.1.52.jar:'\
//     +driver_home+'/mongo/mongo-spark-connector_2.11-2.2.3.jar:'\
//     +driver_home+'/mongo/mongo-java-driver-3.8.0.jar') 

// sc = SparkContext.getOrCreate(conf)

// spark = SQLContext(sc)

val spark = SparkSession.
	builder().
	appName("Spark Multi DB DataPump Test").
	config("spark.driver.memory", "7g").
	config("spark.driver.extraClassPath", "drivers/mssql-jdbc-9.2.1.jre11.jar:drivers/jaybird-full-3.0.9.jar").
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
				option("url", "jdbc:firebirdsql:192.168.1.148/3050:/grsc/Clientes/ClientesGRSA/Rockwell Automation Monterrey/Base de datos 2021/Migracion de BD/ROCKWELL_MIG.FDB").
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


class Column(val name:String, val numeric:Boolean=false, val isKey:Boolean=false, val colLength:Integer=null)
class SchemaTable(val name:String, val conn:Connections=null) {
	if (conn != null) {
		registerDF(conn, name)
	} 
}
class Table(val input:SchemaTable, val output:SchemaTable, val outRows:Seq[(Column, Column)], val select:String=null, val alsoUpdate:Connections=null)

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
		return replace(col(c.name))
	} else {
		var a = col(c.name)
		if(c.colLength != null) {
			a = substring(a, 0, c.colLength)
		}
		return concat(lit("'"), replace( a ), lit("'"))
	}
}

def fmtValue(c:Column):org.apache.spark.sql.Column = {
	when(col(c.name).isNull, "NULL").otherwise(
		encapsulate(c)
	)
}

def genColumn(tb:Table): org.apache.spark.sql.Column = {
	var alsoUpdate:Boolean = tb.alsoUpdate != null

	if (tb.alsoUpdate != null && tb.outRows.filter(rw=>rw._1.isKey).length == 0) {
		alsoUpdate = false;
		println("AlsoUpdate Not Applyed.")
	}

	var outCol: org.apache.spark.sql.Column = concat(lit(

		s"""${if (!alsoUpdate) "INSERT" else {
			tb.alsoUpdate match {
				case Connections.fb => {
					"UPDATE OR INSERT"
				}
			}
		}} INTO ${tb.output.name}(${tb.outRows.map(rw=>rw._2.name).mkString(", ")})
		VALUES("""),
		fmtValue(tb.outRows.head._1)
	)
	for (c <- tb.outRows.tail.map(rw=>rw._1)) {
		outCol = concat(outCol, lit(","), fmtValue(c))
	}
	concat(outCol, lit(s") ${if (!alsoUpdate) "" else {
		tb.alsoUpdate match {
			case Connections.fb => {
				s"MATCHING (${tb.outRows.filter(rw=>rw._1.isKey).map(rw=>rw._2.name).mkString(", ")})"
			}
		}
	}};\n"))
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

def saveBatchTables(names:Seq[String], tables:Seq[Table], batchSize:Int=600) = {
	for ((tb, i) <- tables.view.zipWithIndex) {
		saveBatchTable(names(i), batchSize, tb)
	}
}