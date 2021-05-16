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

registerDFList(Seq(
  (Connections.fb, "PRODUCTOS"),
  (Connections.mssql, "GR_PRODUCTOS_fix_NPARTE")
  )
)

saveBatch("result.sql",
  500,
  spark.sql("select * from GR_PRODUCTOS_fix_NPARTE LEFT JOIN PRODUCTOS ON NPArte = PRODUCTOS.P_NPARTE WHERE P_NPARTE IS NULL").
    map(row => {
      s"""INSERT INTO
    PRODUCTOS (
        P_NPARTE,
        P_DESCESP,
        P_DESCING,
        P_TIPO,
        P_TIPOPARTE,
        P_UMEDIDAPARTE,
        P_UMEDIDAFRACCION,
        P_ARANCELMX,
        P_ARANCELUS,
        P_FACTCONVERSION,
        P_NOTAS,
        P_GENDESP,
        P_GENMERMA
    ) VALUES (
      ${if (row.getAs[String]("NPArte") == null) "NULL" else s"'${row.getAs[String]("NPArte")}'"},
      ${if (row.getAs[String]("DescEsp") == null) "NULL" else s"'${row.getAs[String]("DescEsp")}'"},
      ${if (row.getAs[String]("DescIng") == null) "NULL" else s"'${row.getAs[String]("DescIng")}'"},
      ${if (row.getAs[String]("Tipo") == null) "NULL" else s"'${row.getAs[String]("Tipo")}'"},
      ${if (row.getAs[String]("TipoParte") == null) "NULL" else s"'${row.getAs[String]("TipoParte")}'"},
      ${if (row.getAs[String]("UM") == null) "NULL" else s"'${row.getAs[String]("UM")}'"},
      ${if (row.getAs[String]("UMedidaFraccion") == null) "NULL" else s"'${row.getAs[String]("UMedidaFraccion")}'"},
      ${if (row.getAs[String]("FracMex") == null) "NULL" else s"'${row.getAs[String]("FracMex")}'"},
      ${if (row.getAs[String]("ArancelUs") == null) "NULL" else s"'${row.getAs[String]("ArancelUs")}'"},
      ${if (row.getAs[String]("FactConv") == null) "NULL" else s"'${row.getAs[String]("FactConv")}'"},
      ${if (row.getAs[String]("Nota") == null) "NULL" else s"'${row.getAs[String]("Nota")}'"},
      ${if (row.getAs[String]("Desp") == null) "NULL" else s"'${row.getAs[String]("Desp")}'"},
      ${if (row.getAs[String]("Merm") == null) "NULL" else s"'${row.getAs[String]("Merm")}'"}
    );\n"""
    }))
