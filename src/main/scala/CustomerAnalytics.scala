import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CustomerAnalytics {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 13 - Spark SQL Basics")
      .master("local[*]")
      .getOrCreate()

    // 1. Create DataFrame from CSV
    val customers = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/customers.csv")

    // 2. Inspect Schema
    println("\n==============================")
    println("CUSTOMER DATAFRAME SCHEMA")
    println("==============================")

    customers.printSchema()

    // 3. Display Data
    println("\n==============================")
    println("CUSTOMER DATA")
    println("==============================")

    customers.show()

    // 4. Select columns
    println("\n==============================")
    println("CUSTOMER NAME AND CITY")
    println("==============================")

    customers
      .select("Name", "City", "PurchaseAmount")
      .show()

    // 5. Filter high-value customers
    println("\n==============================")
    println("HIGH VALUE CUSTOMERS")
    println("==============================")

    customers
      .filter(col("PurchaseAmount") > 10000)
      .show()

    // 6. Create new column using withColumn
    val updatedCustomers = customers.withColumn(
      "CustomerType",
      when(col("PurchaseAmount") >= 20000, "Premium")
        .when(col("PurchaseAmount") >= 10000, "Regular")
        .otherwise("Basic")
    )

    println("\n==============================")
    println("CUSTOMER TYPE")
    println("==============================")

    updatedCustomers
      .select("Name", "PurchaseAmount", "CustomerType")
      .show()

    // 7. Create Temporary View
    updatedCustomers.createOrReplaceTempView("customers")

    // 8. Run Spark SQL
    println("\n==============================")
    println("SQL - HIGH VALUE CUSTOMERS")
    println("==============================")

    spark.sql(
      """
        |SELECT Name, City, PurchaseAmount, CustomerType
        |FROM customers
        |WHERE PurchaseAmount > 15000
        |""".stripMargin
    ).show()

    // 9. Average purchase amount
    println("\n==============================")
    println("AVERAGE PURCHASE AMOUNT")
    println("==============================")

    spark.sql(
      """
        |SELECT ROUND(AVG(PurchaseAmount), 2) AS AveragePurchase
        |FROM customers
        |""".stripMargin
    ).show()

    // 10. City-wise analytics
    println("\n==============================")
    println("CITY-WISE ANALYTICS")
    println("==============================")

    spark.sql(
      """
        |SELECT
        |  City,
        |  COUNT(*) AS CustomerCount,
        |  SUM(PurchaseAmount) AS TotalPurchase
        |FROM customers
        |GROUP BY City
        |ORDER BY TotalPurchase DESC
        |""".stripMargin
    ).show()

    spark.stop()
  }
}
