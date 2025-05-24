// Databricks notebook source
// MAGIC %md
// MAGIC ### Ricardo López Rodríguez A01066515
// MAGIC #### 06/12/2021

// COMMAND ----------

// MAGIC %md ## Final evaluation
// MAGIC 
// MAGIC For the last activity of the semester, we have been asked to help design the visit schedule of 50K customers for a large company. In each visit, the company's staff will offer a promotion of some defined products. Previous studies showed that pushing too many promotions per day can lead to the customer feeling overwhelmed.
// MAGIC 
// MAGIC The dataset consists of a list of customers, their applicable products (can be different per customer), and the valid days (valid = 1, not valid = 0) in which each product can be offered. Below is the data we have received.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Objective
// MAGIC 
// MAGIC The task is to define a personalized schedule for each customer, in which every applicable product is offered on one valid day of the week. As there are many possible schedules per customer (and we want to comply with the company's concerns), we will evaluate the "fitness" of the schedule as the maximum number of products offered on any day of the week. The higher the maximum number of offerings in a day, the worst the fitness.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Example
// MAGIC 
// MAGIC Below is an example where the company is scheduling Lex's visits:
// MAGIC 
// MAGIC | CustomerId     | product     | Monday     | Tuesday     | Wednesday     | Thursday     | Friday     | Saturday     |
// MAGIC |:----------:    |:-------:    |:------:    |:-------:    |:---------:    |:--------:    |:------:    |:--------:    |
// MAGIC |     Lex        |    A        |    0       |    1        |     0         |     1        |    0       |     0        |
// MAGIC |     Lex        |    B        |    1       |    0        |     0         |     1        |    0       |     0        |
// MAGIC |     Lex        |    C        |    0       |    1        |     1         |     0        |    0       |     0        |
// MAGIC |     Lex        |    D        |    0       |    0        |     0         |     0        |    1       |     0        |
// MAGIC 
// MAGIC For product A, we have 2 possible days to be suggested, Tuesday and Thursday. Similarly, there is only one day for product D. Let's define the promotion offering schedule as follows:
// MAGIC 
// MAGIC | CustomerId     | Monday     | Tuesday     | Wednesday     | Thursday     | Friday     | Saturday     |
// MAGIC |:----------:    |:------:    |:-------:    |:---------:    |:--------:    |:------:    |:--------:    |
// MAGIC |     Lex        |    B       |   A, C      |     -         |     -        |    D       |     -        |
// MAGIC 
// MAGIC We can observe that the highest amount of products being offered is on Tuesday, being it 2. Therefore, we can evaluate this schedule with a fitness of 2 (maximum number of simultaneous offerings).
// MAGIC 
// MAGIC As we are dealing with 50,000 customers, we will aggregate the fitness metric by taking the average of it across all customers. Let us say that we only have the following two customers:
// MAGIC 
// MAGIC | CustomerId     | Monday     | Tuesday     | Wednesday     | Thursday     | Friday     | Saturday     | Fitness     |
// MAGIC |:----------:    |:------:    |:-------:    |:---------:    |:--------:    |:------:    |:--------:    |:-------:    |
// MAGIC |     Lex        |    B       |   A, C      |     -         |     -        |    D       |     -        |    2        |
// MAGIC |    Yann        |    A       |    P        |     J         |     E        |  B,Y,K     |     -        |    3        |
// MAGIC 
// MAGIC The total fitness of the scheduling heuristic would be (2+3)/2 = 2.5. 

// COMMAND ----------

// MAGIC %md #### Expected output
// MAGIC 
// MAGIC The previous example serves only to explain the problem, however, the expected output schema should comply with the following table:
// MAGIC 
// MAGIC | CustomerId (Long)    | product (Str)    |   day (Str)      | fitness (Int) |
// MAGIC |:----------:        |:-------:        |:-------:        |:-------:        |
// MAGIC |     505            |    A            | Tuesday         |    2            |
// MAGIC |     505            |    B            |  Monday         |    2            |
// MAGIC |     505            |    C            | Tuesday         |    2            |
// MAGIC |     505            |    D            |  Friday         |    2            |
// MAGIC |    6780            |    A            |  Monday         |    3            |
// MAGIC |    6780            |    B            |  Friday         |    3            |
// MAGIC |    6780            |    P            | Tuesday         |    3            |

// COMMAND ----------

// MAGIC %md #### Performance
// MAGIC 
// MAGIC To get the overall performance of your scheduling heuristic, apply the following function to the expected output DataFrame. The lower the overall fitness, the better.

// COMMAND ----------

display(
  output.select("CustomerId", "fitness").distinct.agg(avg("fitness").alias("Overall Performance"))
)

// COMMAND ----------

// MAGIC %md #### Evaluation
// MAGIC The company has a solution implemented already, however, they think that it can be improved. The performance of your submission X will be then compared against the score S of the company's algorithm (which will be shared with you at the end). The score you get will depend on the next table:
// MAGIC 
// MAGIC | Your performance     | Grade     |
// MAGIC |:--------------------:|:---------:|
// MAGIC |    X < S+40          |   70      |
// MAGIC |    X < S+30          |   80      |
// MAGIC |    X < S+20          |   85      |
// MAGIC |    X < S+10          |   90      |
// MAGIC |    X < S+5           |   95      |
// MAGIC |    X <= S            |  100      |
// MAGIC 
// MAGIC Any selection that is worse than S+40 will be graded as 60.

// COMMAND ----------

// MAGIC %md
// MAGIC ## Reading data and initial transformations

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

var data = spark.read.format("csv").option("header",true).load("/FileStore/tables/case_study-1.csv")//Reading csv .. 
                .withColumn("CustomerId",$"CustomerId".cast("int"))
                .withColumn("product",$"product".cast("int"))
                .withColumn("MONDAY",$"MONDAY".cast("int")) //Fixing data types
                .withColumn("TUESDAY",$"TUESDAY".cast("int"))
                .withColumn("WEDNESDAY",$"WEDNESDAY".cast("int"))
                .withColumn("THURSDAY",$"THURSDAY".cast("int"))
                .withColumn("FRIDAY",$"FRIDAY".cast("int")) 
                .withColumn("SATURDAY",$"SATURDAY".cast("int")) //Casting columns                                                    Keep products that at least have
                .filter($"MONDAY" > 0 || $"TUESDAY" > 0 || $"WEDNESDAY" > 0 || $"THURSDAY" > 0 || $"FRIDAY" > 0 || $"SATURDAY" > 0) //one valid day
                .withColumn("TUESDAYID", when($"TUESDAY" === 1, 2).otherwise($"TUESDAY")) //Creatian IDs for days, representing indices in an "array". 
                .withColumn("WEDNESDAYID", when($"WEDNESDAY" === 1, 3).otherwise($"WEDNESDAY")) //Monday already have a value equal to 1
                .withColumn("THURSDAYID", when($"THURSDAY" === 1, 4).otherwise($"THURSDAY"))
                .withColumn("FRIDAYID", when($"FRIDAY" === 1, 5).otherwise($"FRIDAY"))
                .withColumn("SATURDAYID", when($"SATURDAY" === 1, 6).otherwise($"SATURDAY"))
                .withColumn("validDays_array",array($"MONDAY",$"TUESDAYID",$"WEDNESDAYID",$"THURSDAYID",$"FRIDAYID",$"SATURDAYID"))//Create a valid days array
                .withColumn("valid_days_id", expr("array_remove(validDays_array, 0)")) //Saving just the valid days ids
                .withColumn("array_lenght", expr("cardinality(valid_days_id)"))
                .withColumn("randSelector_Index", when($"array_lenght" >= 0, round(rand(seed = 75)*($"array_lenght" - 1) + 1 ))) //random selection 
                .withColumn("randSelector_Index", $"randSelector_Index".cast("int"))
                .withColumn("randSelected_promoDay",expr("element_at(valid_days_id,randSelector_Index)") )
                .drop("TUESDAYID","WEDNESDAYID","THURSDAYID","FRIDAYID","SATURDAYID")


//display(data.filter($"CustomerId" === -1071167120))
display(data)

// COMMAND ----------

//Computing the distribution of valid days for each customer ... 
var newDf = data.groupBy($"CustomerId".as("CustomerId2"))
                .agg(sum("MONDAY"), sum("TUESDAY"), sum("WEDNESDAY"), sum("THURSDAY"), sum("FRIDAY"), sum("SATURDAY")) //frequency of the valid days
                .withColumn("sum",$"sum(MONDAY)" + $"sum(TUESDAY)" + $"sum(WEDNESDAY)" + $"sum(THURSDAY)" + $"sum(FRIDAY)" + $"sum(SATURDAY)")
                .select($"CustomerId2", (($"sum" - $"sum(MONDAY)")/$"sum").as("MondayFreq") , ( ($"sum" -$"sum(Tuesday)")/$"sum" ).as("TuesdayFreq"),
                        (($"sum" - $"sum(WEDNESDAY)")/$"sum" ).as("WednesdayFreq"),  (($"sum"-$"sum(THURSDAY)" )/$"sum").as("ThursdayFreq"), // "Function that
                        (($"sum"-$"sum(FRIDAY)")/$"sum" ).as("FridayFreq"),  (($"sum" - $"sum(SATURDAY)")/$"sum").as("SaturdayFreq")) // gives more weigth to small numbers"
                .withColumn("MondayFreq", when($"MondayFreq" === 1, 0).otherwise($"MondayFreq"))//change days with value 1, 
                .withColumn("TuesdayFreq", when($"TuesdayFreq" === 1, 0).otherwise($"TuesdayFreq"))//since they are not valid days for promo
                .withColumn("WednesdayFreq", when($"WednesdayFreq" === 1, 0).otherwise($"WednesdayFreq"))
                .withColumn("ThursdayFreq", when($"ThursdayFreq" === 1, 0).otherwise($"ThursdayFreq"))
                .withColumn("FridayFreq", when($"FridayFreq" === 1, 0).otherwise($"FridayFreq"))
                .withColumn("SaturdayFreq", when($"SaturdayFreq" === 1, 0).otherwise($"SaturdayFreq"))
                .withColumn("sumOfWeigths",$"MondayFreq" + $"TuesdayFreq"+$"WednesdayFreq"+ $"ThursdayFreq"+$"FridayFreq" + $"SaturdayFreq")
                .select($"CustomerId2", ($"MondayFreq"/$"sumOfWeigths").as("MondayWeight"),($"TuesdayFreq"/$"sumOfWeigths").as("TuesdayWeight"),
                       ($"WednesdayFreq"/$"sumOfWeigths").as("WednesdayWeight"), ($"ThursdayFreq"/$"sumOfWeigths").as("ThursdayWeight"),
                       ($"FridayFreq"/$"sumOfWeigths").as("FridayWeight"),($"SaturdayFreq"/$"sumOfWeigths").as("SaturdayWeight"))
                .withColumn("weights", array($"MondayWeight", $"TuesdayWeight", $"WednesdayWeight", $"ThursdayWeight", $"FridayWeight", $"SaturdayWeight"))
                .withColumn("weights",expr("array_remove(weights,0)")) //delete days with value 0, since they are not valid days for promo
                //.withColumn("sortedWeights", expr("array_sort(weights)"))
                .withColumn("element_1", expr("element_at(weights,1)")) // extraction of single elements to compute the commulative sum
                .withColumn("element_2", expr("element_at(weights,2)"))
                .withColumn("element_3", expr("element_at(weights,3)"))
                .withColumn("element_4", expr("element_at(weights,4)"))
                .withColumn("element_5", expr("element_at(weights,5)"))
                .withColumn("element_6", expr("element_at(weights,6)"))
                .withColumn("element_1", when($"element_1".isNull === true,0).otherwise($"element_1") )  //Impute 0s in missing values
                .withColumn("element_2", when($"element_2".isNull === true,0).otherwise($"element_2") ) 
                .withColumn("element_3", when($"element_3".isNull === true,0).otherwise($"element_3") ) 
                .withColumn("element_4", when($"element_4".isNull === true,0).otherwise($"element_4") ) 
                .withColumn("element_5", when($"element_5".isNull === true,0).otherwise($"element_5") ) 
                .withColumn("element_6", when($"element_6".isNull === true,0).otherwise($"element_6") ) 
                .withColumn("cumSum_e1", $"element_1") //Computing each element of the comulative sum ...
                .withColumn("cumSum_e2", $"cumSum_e1" + $"element_2" )
                .withColumn("cumSum_e3", $"cumSum_e2" + $"element_3" )
                .withColumn("cumSum_e4", $"cumSum_e3" + $"element_4" )
                .withColumn("cumSum_e5", $"cumSum_e4" + $"element_5" )
                .withColumn("cumSum_e6", $"cumSum_e5" + $"element_6" ) //transform single elements into an array
                .withColumn("cumSum_array",array($"cumSum_e1",$"cumSum_e2",$"cumSum_e3",$"cumSum_e4",$"cumSum_e5",$"cumSum_e6"))
                .withColumn("cumSum_array", expr("array_distinct(cumSum_array)")) //just keep unique elements
                .drop("element_1","element_2","element_3","element_4","element_5","element_6",
                     "cumSum_e1","cumSum_e2","cumSum_e3","cumSum_e4","cumSum_e5","cumSum_e6").cache() //drop dummy columns 
                
               // .withColumn("maxVal", expr("array_max(sortedProbabilities)"))
               // .withColumn("arrayWithoutMax", expr("array_remove(sortedProbabilities,maxVal)"))
display(newDf)

// COMMAND ----------

//Computin a join operation between the original df and newDf
var data_df = data.join(newDf, data("CustomerId") === newDf("CustomerId2"), "inner").drop("CustomerId2").cache()
display(data_df)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Roulette selection

// COMMAND ----------

// Final selection based on the roulette game (Weigthed proporsionete selecction)
//If a product have just one valid promo day, that day is selected
//If a product have less valid days than the number of weights, uniform random selection is used
//If a product have the same number of valid days that the number of weights, then roulette selection is used
var finalSelectionDf = data_df
                  .withColumn("numberOfWeights", expr("cardinality(weights)"))
                  .withColumn("elementCum_1", expr("element_at(cumSum_array,1)")) // extraction of single elements to compute the commulative sum
                  .withColumn("elementCum_2", expr("element_at(cumSum_array,2)"))
                  .withColumn("elementCum_3", expr("element_at(cumSum_array,3)"))
                  .withColumn("elementCum_4", expr("element_at(cumSum_array,4)"))
                  .withColumn("elementCum_5", expr("element_at(cumSum_array,5)"))
                  .withColumn("elementCum_6", expr("element_at(cumSum_array,6)"))
                  .withColumn("elementCum_1", when($"elementCum_1".isNull === true,0).otherwise($"elementCum_1") )  //Impute 0s in missing values
                  .withColumn("elementCum_2", when($"elementCum_2".isNull === true,0).otherwise($"elementCum_2") ) 
                  .withColumn("elementCum_3", when($"elementCum_3".isNull === true,0).otherwise($"elementCum_3") ) 
                  .withColumn("elementCum_4", when($"elementCum_4".isNull === true,0).otherwise($"elementCum_4") ) 
                  .withColumn("elementCum_5", when($"elementCum_5".isNull === true,0).otherwise($"elementCum_5") ) 
                  .withColumn("elementCum_6", when($"elementCum_6".isNull === true,0).otherwise($"elementCum_6") )   
                  .withColumn("Dice", rand(seed = 42)) //random number
                  .withColumn("finalSelector_indices",  //Final selected indices 
                  when($"numberOfWeights" === $"array_lenght",  //playing the roulette game
                       when($"Dice" < $"elementCum_1", 1).
                       when($"Dice" < $"elementCum_2",2).
                       when($"Dice" < $"elementCum_3", 3).
                       when($"Dice" < $"elementCum_4", 4).
                       when($"Dice" < $"elementCum_5",5).
                       when($"Dice" < $"elementCum_6", 6)
                      )
                   .otherwise($"randSelector_Index") //random index
                  
                  
                  )
                  .withColumn("finalSelected_promoDay",expr("element_at(valid_days_id,finalSelector_indices)") ) //Selection of promoDay

display(finalSelectionDf)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Final data frame and fitness

// COMMAND ----------

//Computing fitness
var computing_fitness = finalSelectionDf.groupBy($"CustomerId".as("CustomerIdDummy"),$"finalSelected_promoDay".as("finalSelected_promoDayDummy"))
                                         .count()
                                         .groupBy("CustomerIdDummy").agg(max("count").as("fitness"))
//join
//FINAL DATA FRAME...
var output = finalSelectionDf.join(computing_fitness,finalSelectionDf("CustomerId") === computing_fitness("CustomerIdDummy"), "inner").drop("CustomerIdDummy")
                                      .withColumn("M", lit("Monday")) //Giving the final desiered format to the dataFrame 
                                      .withColumn("T", lit("Tuesday"))
                                      .withColumn("W", lit("Wednesday"))
                                      .withColumn("TH", lit("Thursday"))
                                      .withColumn("F", lit("Friday"))
                                      .withColumn("S", lit("Saturday"))
                                      .withColumn("str_days_array", array($"M",$"T",$"W",$"TH",$"F",$"S"))
                                      .withColumn("day", expr("element_at(str_days_array,finalSelected_promoDay)"))
                                      .withColumn("product", $"product".cast("string"))
                                      .select("CustomerId","product","day","fitness")  //selection of the desiered final columns ...
display(output)

// COMMAND ----------

display(
  output.select("CustomerId", "fitness").distinct.agg(avg("fitness").alias("Overall Performance"))
)