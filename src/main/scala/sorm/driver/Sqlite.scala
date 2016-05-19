package sorm.driver

import sorm._, ddl._, jdbc._
import sext._, embrace._
import sql.Sql

class Sqlite (protected val connection : JdbcConnection)
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

  override protected def showTablesSql
  = "SELECT TBL_NAME FROM SQLITE_MASTER WHERE TYPE = 'table'"

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
}
