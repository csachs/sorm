package vorm

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import vorm._
import api._
import persisted._
import query._
import reflection._
import save._
import structure._
import mapping._
import jdbc._
import create._
import extensions._

import samples._
import com.codahale.logula.Logging
import org.apache.log4j.Level

@RunWith(classOf[JUnitRunner])
class SeqOfSeqsOfIntsSupportSuite extends FunSuite with ShouldMatchers {

  import SeqOfSeqsOfIntsSupportSuite._

  Logging.configure { log =>
    log.level = Level.TRACE
    log.loggers("vorm.jdbc.ConnectionAdapter") = Level.TRACE
  }

  test("An empty item Seq does not match inexistent one"){
    db.query[A]
      .filterEquals("a.item", Seq())
      .fetchAll().view.map{_.id}.toSet 
      .should(
        not contain (1l) and
        not contain (5l)
      )
  }
}
object SeqOfSeqsOfIntsSupportSuite {
  case class A ( a : Seq[Seq[Int]] )

  val db = TestingInstance.h2( Entity[A]() )
  db.save(A( Seq() ))
  db.save(A( Seq( Seq(2, 3), Seq(), Seq(7) ) ))
  db.save(A( Seq( Seq() ) ))
  db.save(A( Seq( Seq(78) ) ))
  db.save(A( Seq() ))

  def fetchIdsWithEqualingItems
    ( value : Seq[_] )
    : Set[Long]
    = db.query[A]
        .filterEquals("a.item", Seq())
        .fetchAll().view.map{_.id}.toSet 
  def fetchEqualingIds ( value : Seq[_] ) : Set[Long]
    = db.query[A].filterEquals("a", value).fetchAll().map{_.id}.toSet
  def fetchNotEqualingIds ( value : Seq[_] ) : Set[Long]
    = db.query[A].filterNotEquals("a", value).fetchAll().map{_.id}.toSet
}