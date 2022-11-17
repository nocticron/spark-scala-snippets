# Snippets

- Assume that you want to apply function $a$ to columns `address` and `name`, and $b$ to `hash`:
```scala
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.regexp_replace

val a = (x: Column) => regexp_replace(x, "a", "")
val b = (x: Column) => regexp_replace(x, "b", "")

val applies = Map(
    Seq("address", "name") -> a,
    Seq("hash") -> b
)

val df = Seq(("a","b","c"))
    .toDF("address", "hash", "name")

val dfWithColumns = (
        df: DataFrame, 
        applies:  Map[_ <: Iterable[String], (Column => Column)]
    ) => 
    applies.foldLeft(df)(
        (oldDf, sequenceOfCols) => 
            sequenceOfCols._1.foldLeft(oldDf)(
                (dft, currentCol) =>  
                    dft.withColumn(currentCol, sequenceOfCols._2(col(currentCol)))
            )
        )

val dft = dfWithColumns(df, applies)
```
```scala
dft.limit(1).show
+-------+----+----+
|address|hash|name|
+-------+----+----+
|       |    |   c|
+-------+----+----+
```

- Most common config params
```conf
spark.app.name=LOLKEK
spark.dynamicAllocation.maxExecutors=15
spark.driver.memory=8G
spark.executor.memoryOverhead=2G
```
- Drop table not using (usually unescaped) SQL
```scala
import org.apache.spark.sql.execution.command.DropTableCommand
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.TableIdentifier

val p: ParserInterface = spark.sessionState.sqlParser
val targetIdentifier: TableIdentifier = p.parseTableIdentifier("a.b")

// or manually:
// val targetIdentifier = TableIdentifier(tableName, Some(db))

val res = DropTableCommand(tableName=targetIdentifier, ifExists=true, isView=false, purge=false).run(spark)  
```

- Read file
```python
import json
from pyspark import SparkFiles

spark.sparkContext.addFile("hdfs://a.json")
with open(SparkFiles.get('a.json'), 'rb') as handle:
    j = json.load(handle)
```

- Set Zeppelin Job name
```scala
spark.sparkContext.setJobGroup(z.getInterpreterContext.getNoteId, "Moving data")
```

- Export SQL statement from views
```scala
import org.apache.spark.sql.execution.command.ShowCreateTableCommand
import org.apache.spark.sql.catalyst.TableIdentifier

val printStatement = (db: String, table: String) => ShowCreateTableCommand(
    TableIdentifier(table, Some(db))).run(spark)(0)(0).toString

val res = spark.catalog.listTables(db).where(col("tableType")===lit("VIEW"))
    .collect.map((x) => x.name).map(x => printStatement(db, x))
```

- Parse dates
```scala
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

val PARTITION_FORMAT = "yyyy-MM-dd"
val fmt = DateTimeFormatter.ofPattern(PARTITION_FORMAT)

val dateFrom = "2022-01-01"
val a = LocalDate.parse(dateFrom, fmt)

val dateTo = "2022-01-01"
val b =  LocalDate.parse(dateTo, fmt)

val months = ChronoUnit.MONTHS.between(a,b)
```
- Broadcast Iterable
```scala
val targetsList = spark.sparkContext
    .broadcast(Array[String])
```
- Get current config

```scala
spark.sparkContext.getConf.getAll.mkString("\n")
```

- Compute df size in bytes (from stackoverflow)
```scala
df.cache.foreach(_ => ())
val catalyst_plan = df.queryExecution.logical
val df_size_in_bytes = spark.sessionState.executePlan(
    catalyst_plan).optimizedPlan.stats.sizeInBytes
print(df_size_in_bytes)
```

- Stable union (no types validation!)
```scala
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.DataFrame

val tables = Seq(a,b,c,df)
val columns = Seq("cola", "colb").map(col)
tables.map( (x: DataFrame) => x.select(columns:_*)).reduce(_ union _)
```

- Check the level height. A row has a level height $H$, if `level_1` to `level_H` are $Not Null$, and `level_H+1` to `level_MAX_LEVEL` are $Null$. The $Level_{MAX}$ if known beforehand. The task is, having the `level` height column and `level_i` columns, check that height number is correct.
> e.g. level height: 5, max: 7, so columns `level_1`, ..., `level_5` are not null, and `level_6` and `level_7` are null

```scala
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, when, lit}

val LEVELS = 7
val levels = (for (i <- 1 to LEVELS) yield s"level_$i")

/* Given the level number, yield the checking statement for it. */
val genChecker_ = (level: Int, maxLevel: Int) => {
    if (level>0) {
        val yes = for (i <- 1 to level) yield col(s"level_$i").isNotNull
        if (level<maxLevel) {
            val no = for (i <- level+1 to maxLevel) yield col(s"level_$i").isNull
            (yes.reduce(_ and _) and no.reduce(_ and _))        
        } else yes.reduce(_ and _) 
    } else lit(false)
}

/* Config the maxLevel */
val genChecker = (level:Int) => genChecker_(level, LEVELS)

val levelsToGen = for (i <- 2 to LEVELS) yield i // cause of using foldLeft, hack to count from 2

/* Since the levels count is finite, you can pre-generate this to a Map-like constant */
val correctHeight = (x: Column) => levelsToGen.foldLeft(
        when(x===lit(1), genChecker(1))
    ){
        (thatWhen, currentNumber) => thatWhen.when(x===lit(currentNumber), genChecker(currentNumber))
    }.otherwise(lit(false)) // chain all whens: when(a).when(b).when(c)...otherwise(false)
```
