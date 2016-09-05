package sorm.driver

import java.sql.{Connection, SQLException}
import java.util.regex.Pattern

import sorm._
import ddl._
import jdbc._
import sext._
import embrace._
import sql.Sql
import org.joda.time.DateTime
import sorm.sql.Sql.{Delete, Insert, Union}

class Sqlite (protected val rawConnection : JdbcConnection)
  extends DriverConnection
    with StdConnection
    with StdTransaction
    with StdAbstractSqlToSql
    with StdQuote
    with StdSqlRendering
    with StdStatement
    with StdQuery
    with StdModify
    with StdCreateTable
    with StdListTables
    with StdDropTables
    with StdDropAllTables
    with StdNow
{

  class SqliteJdbcConnection ( override protected val connection : Connection ) extends JdbcConnection(connection) {

    override def executeUpdateAndGetGeneratedKeys
    ( s : Statement )
    : List[IndexedSeq[Any]]
    = {
      logStatement(s)
      val js = preparedStatement(s, true)
      executeLoggingBenchmark(js.executeUpdate())
      val rs = js.getGeneratedKeys
      val r = rs.indexedRowsTraversable.toList
      rs.close()
      js.close()
      r
    }
  }

  val connection = new SqliteJdbcConnection(rawConnection.getConnection())

  override protected def showTablesSql
  = "SELECT TBL_NAME FROM SQLITE_MASTER WHERE TYPE = 'table' AND TBL_NAME != 'sqlite_sequence'"

  override protected def tableDdl ( t : Table ) : String
  = {
    val Table(name, columns, primaryKeyStream, uniqueKeys, indexes, foreingKeys) = t
    val primaryKey = primaryKeyStream.toSet
    val statements =
      ( columns.map(columnDdl) ++:
        primaryKey.toList.map(
          name => (name, columns.find(_.name == name).get.autoIncrement)
        ).$(primaryKeyAndIncrementDdl) +:
        indexes.map(indexDdl).filter(_.nonEmpty) ++:
        uniqueKeys.map(uniqueKeyDdl) ++:
        foreingKeys.map(foreingKeyDdl).toStream
        ) .filter(_.nonEmpty)

    "CREATE TABLE " + quote(name) +
      ( "\n( " + statements.mkString(",\n").indent(2).trim + " )" ).indent(2)
  }

  protected def primaryKeyAndIncrementDdl ( columns : Seq[(String, Boolean)] )
  = "PRIMARY KEY (" + columns.view.map {
    case (name, isIncrement) => quote(name) + isIncrement.option(" AUTOINCREMENT ").getOrElse("")
  }.mkString(", ") + ")"

  override protected def columnDdl ( c : Column )
  = quote(c.name) + " " + columnTypeDdl(c.autoIncrement.option(ColumnType.Integer).getOrElse(c.t)) +
    c.nullable.option(" NULL").getOrElse(" NOT NULL")

  override def insertAndGetGeneratedKeys( table : String, values : Iterable[(String, Any)] ) : Seq[Any] =
    super.insertAndGetGeneratedKeys(table, values).map {
      case _id: Long => _id
      case _id: Int => _id.toLong
    }

  override protected def template ( sql : Sql ) : String
    = sql match {
    case Insert(table, columns, values) if columns.isEmpty && values.isEmpty =>
      "INSERT INTO " + quote(table) + " DEFAULT VALUES"
    case Union(l, r) =>
      " " + template(l).indent(2).trim + " \n" +
        "UNION\n" +
        " " + template(r).indent(2).trim + " \n"
    case _ => super.template(sql)
  }

  override def now() : DateTime
    = DateTime.parse(connection
        .executeQuery(Statement("SELECT strftime('%Y-%m-%dT%H:%M:%S','now')"))()
        .head.head
        .asInstanceOf[String])

}

/*
import org.sqlite.{Function, SQLiteConnection}

//Function.create(CONNECTION, classOf[Regexp].getSimpleName, new Regexp())

class Regexp() extends Function() {
  override protected def xFunc = {
    if (args() != 2) {
      throw new SQLException("")
    }

    val pattern = value_text(0)
    val value = value_text(1)

    result(if(Pattern.matches(pattern, value)) 1 else 0)
  }
}
*/

