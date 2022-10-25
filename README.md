# Snippets

- Most common config params
```conf
spark.app.name=LOLKEK
spark.dynamicAllocation.maxExecutors=15
spark.driver.memory=8G
spark.executor.memoryOverhead=2G
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

- Check the level height. A row has a level height $H$, if `level_1` to `level_H` are $1$, and `level_H+1` to `level_MAX_LEVEL` are $0$. The $Level_{MAX}$ if known beforehand.

```scala
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, when, lit}

val LEVELS = 6
val levels = (for (i <- 1 to LEVELS) yield s"category_$i")

val genChecker_ = (level: Int, maxLevel: Int) => {
    if (level>0) {
        val yes = for (i <- 1 to level) yield col(s"category_$i").isNotNull
        if (level<maxLevel) {
            val no = for (i <- level+1 to maxLevel) yield col(s"category_$i").isNull
            (yes.reduce(_ and _) and no.reduce(_ and _))        
        } else yes.reduce(_ and _) 
    } else lit(false)
}
val genChecker = (level:Int) => genChecker_(level, LEVELS) // config the maxLevel 
val levelsToGen = for (i <- 2 to LEVELS) yield i // cause of using foldLeft, hack to count from 2
val correctHeight = (x: Column) => levelsToGen.foldLeft(
        when(x===lit(1), genChecker(1))
    ){
        (thatWhen, currentNumber) => thatWhen.when(x===lit(currentNumber), genChecker(currentNumber))
    }.otherwise(lit(false)) // chain all whens: when(a).when(b).when(c)...otherwise(false)
```