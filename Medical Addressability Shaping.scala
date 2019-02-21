
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

//setup formatters and spark engine
val formatter = java.text.NumberFormat.getCurrencyInstance
val performatter = java.text.NumberFormat.getPercentInstance
val spark = SparkSession.builder.
  master("local[*]").
  config("spark.executor.memory", "8g").
  config("spark.driver.memory", "16g").
  config("spark.memory.offHeap.enabled",true).
  config("spark.memory.offHeap.size","16g").
  appName("Monthly Parquet ETL").
  getOrCreate()

import spark.implicits._
spark.sparkContext.setLogLevel("ERROR")

//read in FPDS archive
val gwcm_feb_19 = spark.read.
  option("header", "true").
  option("inferSchema", "true").
  option("sep", "\t").csv("/Users/cliftonjbest/Documents/Development/GWCM_Extracts/GWCM_CURRENT_MONTH_FEBRUARY_02072019.tsv")

gwcm_feb_19.printSchema()

//read in psc_naics combos for each contract
val Hearing_Aides_add_sigs = gwcm_feb_19.
  filter($"date_signed" >= "2015-10-01").
  filter($"contract_name"=== "Hearing Aids").
  select($"bic_addressability_key").
  distinct().
  collect()

val JNP_add_sigs = gwcm_feb_19.
  filter($"date_signed" >= "2015-10-01").
  filter($"contract_name"=== "JNP").
  select($"bic_addressability_key").
  distinct().
  collect()

val HTME_add_sigs = gwcm_feb_19.
  filter($"date_signed" >= "2015-10-01").
  filter($"contract_name"=== "High-Tech Medical Equipment (HTME)").
  //filter($"bic_addressability_key" =!= null).
  //filter($"naics_code" !== null).
  select($"bic_addressability_key").
  distinct().
  collect()


//write distinct pscs and naics to vectors
var jnp_psc_vector: List[String] = List()
var jnp_naics_vector: List[String] = List()
var hearing_aids_psc_vector: List[String] = List()
var hearing_aids_naics_vector: List[String] = List()
var htme_psc_vector: List[String] = List()
var htme_naics_vector: List[String] = List()

for(inner <- Hearing_Aides_add_sigs)
{
  if(inner.getString(0) != null) {
    //println("pscs "+ inner.getString(0))
    val this_row = inner.getString(0).split("_")
    println("Hearing Aids "+this_row(0) +" "+ this_row(1))
    hearing_aids_psc_vector = hearing_aids_psc_vector :+ this_row(0)
    hearing_aids_naics_vector = hearing_aids_naics_vector :+ this_row(1)
  }

}


for(inner <-JNP_add_sigs)
{
  if(inner.getString(0) != null) {
    //println("pscs "+ inner.getString(0))
    val this_row = inner.getString(0).split("_")
    println("JNP "+ this_row(0) +" "+ this_row(1))
    jnp_psc_vector = jnp_psc_vector :+ this_row(0)
    jnp_naics_vector = jnp_naics_vector :+ this_row(1)
  }

}

for(inner <-HTME_add_sigs)
{
  if(inner.getString(0) != null) {
    //println("pscs "+ inner.getString(0))
    val this_row = inner.getString(0).split("_")
    println("HTME "+this_row(0) +" "+ this_row(1))
    htme_psc_vector = htme_psc_vector :+ this_row(0)
    htme_naics_vector = htme_naics_vector :+ this_row(1)
  }

}


//get addressability

val jnp_current_addressability = gwcm_feb_19.
  filter($"date_signed" >= "2017-10-01" && $"date_signed" <= "2018-09-30").
  filter($"product_or_service_code".isin(jnp_psc_vector.distinct.toList:_*) === true && $"naics_code".isin(jnp_naics_vector.distinct.toList:_*) === true).  //jnp_psc_vector.distinct.toList
  select($"dollars_obligated").
  agg(sum($"dollars_obligated")).collect()


val jnp_addressability = gwcm_feb_19.
  filter($"date_signed" >= "2017-10-01" && $"date_signed" <= "2018-09-30").
  filter($"level_1_category_group" === "GWCM").
  filter($"product_or_service_code" === "6505" && $"naics_code"==="325412" ).
  select($"dollars_obligated").
  agg(sum($"dollars_obligated")).collect()


println("jnp_current_addressability: "+ formatter.format( jnp_current_addressability(0).getDouble(0)))
println("jnp_addressability: "+ formatter.format( jnp_addressability(0).getDouble(0)))

val hearing_aids_current_addressability = gwcm_feb_19.
  filter($"date_signed" >= "2017-10-01" && $"date_signed" <= "2018-09-30").
  filter($"product_or_service_code".isin(hearing_aids_psc_vector.distinct.toList:_*) === true && $"naics_code".isin(hearing_aids_naics_vector.distinct.toList:_*) === true).  //jnp_psc_vector.distinct.toList
  select($"dollars_obligated").
  agg(sum($"dollars_obligated")).collect()

val hearing_aids_addressability = gwcm_feb_19.
  filter($"date_signed" >= "2017-10-01" && $"date_signed" <= "2018-09-30").
  filter($"level_1_category_group" === "GWCM").
  filter($"product_or_service_code" === "6515").
  filter($"naics_code" === "334510").
  select($"dollars_obligated").
  agg(sum($"dollars_obligated")).collect()


println("hearing_aids_current_addressability: "+ formatter.format( hearing_aids_current_addressability(0).getDouble(0)))
println("hearing_aids_addressability: "+ formatter.format( hearing_aids_addressability(0).getDouble(0)))

val htme_current_addressability = gwcm_feb_19.
  filter($"date_signed" >= "2017-10-01" && $"date_signed" <= "2018-09-30").
  filter($"product_or_service_code".isin(htme_psc_vector.distinct.toList:_*) === true && $"naics_code".isin(htme_naics_vector.distinct.toList:_*) === true).  //jnp_psc_vector.distinct.toList
  select($"dollars_obligated").
  agg(sum($"dollars_obligated")).collect()


val htme_psc_naics_list : List[String] = List("6525_334517","6640_334517","J065_334517","6515_334517","6525_423450","7030_334517","J065_334510","J070_339112","6525_334510","J059_334517","J065_339112","J065_423450","J066_334517","N065_334517","6525_339112","L065_334517")

val htme_psc_list : List[String] = List("6525", "6640", "7030", "J065", "J066", "J070", "L065")
val htme_naics_list : List[String] = List("334517", "334510", "339112", "423450")

val htme_addressability = gwcm_feb_19.
  filter($"date_signed" >= "2017-10-01" && $"date_signed" <= "2018-09-30").
  filter($"level_1_category_group" === "GWCM").
  filter($"product_or_service_code".isin(htme_psc_list:_*) === true && $"naics_code".isin(htme_naics_list:_*) === true).
  select($"dollars_obligated").
  agg(sum($"dollars_obligated")).collect()

println("htme_current_addressability: "+ formatter.format(htme_current_addressability(0).getDouble(0)))
println("htme_addressability: "+ formatter.format(htme_addressability(0).getDouble(0)))


