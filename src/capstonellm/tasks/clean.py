import argparse
import json
import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):
    

    questions_raw = spark.read.option("multiLine", "true").json("data/questions.json")
    answers_raw = spark.read.option("multiLine", "true").json("data/answers.json")

    questions = questions_raw.select(F.explode("items").alias("q")).select(
        F.col("q.question_id").alias("question_id"),
        F.col("q.title").alias("title"),
        F.col("q.body").alias("question"),
        F.col("q.link").alias("link"),
        F.col("q.tags").alias("tags"),
    )

    answers = answers_raw.select(F.explode("items").alias("a")).select(
        F.col("a.question_id").alias("question_id"),
        F.col("a.answer_id").alias("answer_id"),
        F.col("a.body").alias("answer"),
        F.col("a.score").alias("score"),
        F.col("a.is_accepted").alias("is_accepted"),
    )

    window = Window.partitionBy("question_id").orderBy(
        F.col("is_accepted").desc(), F.col("score").desc()
    )
    best_answers = (
        answers.withColumn("rank", F.row_number().over(window))
        .filter(F.col("rank") == 1)
        .drop("rank", "score", "is_accepted")
    )

    cleaned = questions.join(best_answers, on="question_id", how="inner").select(
        "question_id", "question", "title", "link", "answer_id", "answer", "tags"
    )

    output_dir = "output/cleaned"
    os.makedirs(output_dir, exist_ok=True)
    for row in cleaned.collect():
        data = row.asDict()
        with open(f"{output_dir}/{data['question_id']}.json", "w") as f:
            json.dump(data, f)

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        builder = SparkSession.builder.appName("Spark S3 Integration").config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2"
        )
        for key, value in common_spark_config.items():
            builder = builder.config(key, value)
        session = builder.getOrCreate()
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()
