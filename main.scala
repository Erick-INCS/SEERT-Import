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
      // option("url", "jdbc:firebirdsql://192.168.1.148/grsc/Clientes/ClientesGRSA/Rockwell Automation Monterrey/Base Datos 2020/Rockwell-UPD-31Dic-2020.fdb").
      option("url", "jdbc:firebirdsql:192.168.1.148/3050:/grsc/Clientes/ClientesGRSA/Rockwell Automation Monterrey/Base Datos 2020/Rockwell-UPD-31Dic-2020.fdb").
      option("user", "sysdba").
      option("password", "masterkey").
      option("dbtable", table).
      load()
  } else {
    spark.read.
      format("jdbc").
      option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
      option("url", "jdbc:sqlserver://192.168.1.151:1433;databaseName=SEERT_ROCKWELL_3_OCT26;").
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

def saveBatch(name:String, batchSize:Int, content:org.apache.spark.sql.Dataset[String]) = {
  for ((b, i) <- content.collect.grouped(batchSize).toList.view.zipWithIndex) {
    save(s"data/${i}_${name}", b.mkString)
  }
}

def getValueOf(row:org.apache.spark.sql.Row, name:String, isNumber:Boolean=false) : String = {
  val index:Int = row.schema.fieldIndex(name)
  if (row.isNullAt(index)) {
    return "NULL"
  }

  row.schema(index).dataType.typeName match {
    case "string" => {
      s"'${row.getAs[String](name).replace("'", "''").replace("\r", "\\r").replace("\n", "\\n")}'"
    }
    case "integer" => {
      s"${row.getAs[Int](name)}"
    }
  }
}