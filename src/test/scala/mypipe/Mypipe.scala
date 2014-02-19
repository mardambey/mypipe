package pillars

import org.specs2.mutable._
import org.specs2.specification.Example
import pillars.mmap.MmapTable
import java.io.File
import pillars.api.{ Column, Row, Table }
import java.util.Random
import pillars.api.Table

object DataGenerator {

  def generateDataFor(table: Table, size: Int) {

    val row = table.createRow()
    val bidPrice = table.acquireColumn("bidPrice")
    val askPrice = table.acquireColumn("askPrice")
    val rand = new Random(size)
    var bp = 1000L
    var ap = 1001L
    val tickSize = 10

    assert(table.size() == 0)

    for (i ← 0 until size) {

      bp += 1 //(rand.nextInt(1) - rand.nextInt(1)) * tickSize
      ap += 1 //(rand.nextInt(1) - rand.nextInt(1)) * tickSize

      //if (ap <= bp) {
      //  ap += tickSize
      //  bp -= tickSize
      //}

      //if (ap > bp + 10 * tickSize) {
      //  if (rand.nextBoolean() || bp < 500)
      //    bp += tickSize
      //  else
      //    ap -= tickSize
      //}
      row.addEntry(i * 10)
      row.set(bidPrice, bp)
      row.set(askPrice, ap)

    }

    assert(table.size() == size)
  }
}

object PillarsTest extends Specification {

  def testReadAndWrite(): Example = {
    "must be able to read and write data " in {
      testMmap() must beEqualTo(true)
    }
  }

  "Pillars" should {
    testReadAndWrite()
  }

  def cleanUp() {
    System.gc()
  }

  def testMmap(): Boolean = {

    val tableName = "bids"
    val start = System.nanoTime()
    val table = new MmapTable(tableName)
    DataGenerator.generateDataFor(table, 10) //0 * 1000 * 1000)
    table.close()
    val time = System.nanoTime() - start
    println(s"MMAP: Took ${time / 1e9} seconds to generate and save ${table.size()} entries")

      //for (i ← 0 until 3) {
      //  val start2 = System.nanoTime()
      //  val table2 = new MmapTable(tableName)
      //  computeMidPriceBP(table2)
      //  System.gc()
      //  table2.close()
      //  val time2 = System.nanoTime() - start2
      //  println(s"MMAP: Took ${time2 / 1e9} seconds calculate the mid BP, plus a GC and save ${table2.size()} entries");
      //}

      def selectTest() {
        import pillars.sql.SQL.{ Select, Term, Table }
        val query = Select(List(Term("bidPrice"), Term("askPrice")), Table("bids"))
        val results = query.execute()

        results foreach println
      }

    selectTest()
    deleteRec(Conf.BASEDIR);

    true
  }

  def deleteRec(dir: String) {
    val file = new File(dir)

    if (file.isDirectory()) file.listFiles().foreach(f ⇒ deleteRec(f.toString))
    file.delete()
  }

  def computeMidPriceBP(table: Table) {
    val row = table.createRow()
    val bidPrice = table.acquireColumn("bidPrice")
    val askPrice = table.acquireColumn("askPrice")
    val midBP = table.acquireColumn("midBP")
    val lastMp = (row.get(bidPrice) + row.get(askPrice)) / 2
    while (row.nextRecord()) {
      val mp = (row.get(bidPrice) + row.get(askPrice)) / 2
      val mpbp = 10000 * (mp - lastMp) / lastMp
      row.set(midBP, mpbp)
    }
  }
}
