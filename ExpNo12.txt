// There are two codes in the fie run any one 
//CODE1
import org.apache.spark.sql.SparkSession
object EvenOddNumbers {
  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("EvenOddNumbers")
      .master("local[*]")
      .getOrCreate()

    // Create RDD from a list of numbers
    val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val numbersRDD = spark.sparkContext.parallelize(numbers)

    // Filter even and odd numbers
    val evenNumbersRDD = numbersRDD.filter(_ % 2 == 0)
    val oddNumbersRDD = numbersRDD.filter(_ % 2 != 0)

    // Collect and print even numbers
    println("Even numbers:")
    evenNumbersRDD.collect().foreach(println)

    // Collect and print odd numbers
    println("Odd numbers:")
    oddNumbersRDD.collect().foreach(println)

    // Stop SparkSession
    spark.stop()
  }
}

//---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------


//CODE2



import org.apache.spark.sql.SparkSession
object ArithmeticOperations {
  def main(args: Array[String]): Unit = {
    print("Enter two numbers : ")
    val num1 = StdIn.readLine().toDouble
    val num2 = StdIn.readLine().toDouble
    println("Select an operation : ")
    println("1. Addition")
    println("2. Subtraction")
    println("3. Multiplication")
    println("4. Division")
    val choice = StdIn.readLine().toInt
    val result = choice match {
      case 1 => num1 + num2
      case 2 => num1 - num2
      case 3 => num1 * num2
      case 4 => if (num2 != 0) num1 / num2 else "Cannot divide by zero"
      case _ => "Invalid choice"
    }
    println(s"Result: $result")
  }
}
