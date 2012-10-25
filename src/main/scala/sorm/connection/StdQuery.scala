package sorm.connection

import sext._, embrace._

import sorm._
import jdbc._
import sql._

trait StdQuery {
  protected def connection: JdbcConnection

  import abstractSql.AbstractSql._
  def query
    [ T ]
    ( asql : Statement )
    ( parse : ResultSetView => T = (_ : ResultSetView).indexedRowsTraversable.toList )
    : T
    = connection.executeQuery(statement(asql))(parse)

  protected def statement(asql: Statement): jdbc.Statement
    = asql $ sql $ Optimization.optimized $ statement
  protected def statement(sql: Sql.Sql): jdbc.Statement
  protected def sql(asql: Statement): Sql.Statement

}