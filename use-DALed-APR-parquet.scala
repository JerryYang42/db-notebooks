// Databricks notebook source
case class Affiliation(id: String, parentId: String, name: String, country: String, city: String)
case class AuthorInfo(affiliations: Seq[Affiliation],
                      pubYearFirst: Int,
                      pubYearLast: Int,
                      givenName: String,
                      surname: String,
                      id: String,
                      email: String,
                      ASJC: Map[Int,Int],
                      SUBJABBR: Map[String,Int])

// COMMAND ----------

val currentDatasource = spark.read.parquet("s3://com-elsevier-recs-dev-reviewers/apr-parsed/20230425-1023/data/").limit(1000).as[AuthorInfo].persist()
display(currentDatasource)

// COMMAND ----------

import java.sql.Timestamp
case class ScopusAuthorProfile(ASJC: Option[Seq[String]],
                               ASJC_frequency_I: Option[Seq[Int]],
                               SUBJABBR: Option[Seq[Option[String]]],
                               SUBJABBR_frequency_I: Option[Seq[Int]],
                               affiliation_current_full: Option[Seq[AffiliationCurrentFull]],
                               alias_status: Option[String],
                               auid: Long,
                               corrupt_xml_file_B: Boolean,
                               current_affiliations: Option[Seq[Long]],
                               current_affiliations_parent: Option[Seq[Option[Long]]], // can be `null` or [null] or [012435, null]
                               datetime_max: String,
                               e_address: Option[String],
                               e_address_type: Option[String],
                               fname: Option[String],
                               given_name_pn: Option[String],
                               history: Option[Seq[Long]],
                               indexed_name_pn: Option[String],
                               initials_pn: Option[String],
                               manual_curation: Option[ManualCuration],
                               n_affiliation_current: Short,
                               name_variants: Option[Seq[NameVariants]],
                               orcid: Option[String],
                               orcid_matching_type: Option[String],
                               suppress: Boolean,
                               surname_pn: Option[String],
                               timestamp_date: String,
                               `type`: String,
                               xmlsize: Long
                              )

case class AffiliationCurrentFull(address_part: Option[String],
                                  afdispname: Option[String],
                                  affiliation_id: Long,
                                  affiliation_id_parent: Option[Long],
                                  afid: Long,
                                  city: Option[String],
                                  country: Option[String],
                                  country_tag: Option[String],
                                  postal_code: Option[String],
                                  preferred_name: Option[String],
                                  relationship: String,
                                  sort_name: Option[String],
                                  state: Option[String],
                                  type_afid: String
                                 )


case class ManualCuration(curated: Boolean,
                          curtype: String,
                          source: String,
                          timestamp: Timestamp
                         )

case class NameVariants(initals: String, //typo at source
                        indexed_name: String,
                        surname: String,
                        given_name: String
                      )

// COMMAND ----------

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.BooleanType
val newDatasource = spark.read.parquet("/mnt/els/automount/author-profiles-multi-column-parquet")
.limit(1000)
.withColumn("suppress", col("suppress").cast(BooleanType))
.as[ScopusAuthorProfile]
.persist()
// .filter($"citations"=!= null)
display(newDatasource)

// COMMAND ----------



// COMMAND ----------


