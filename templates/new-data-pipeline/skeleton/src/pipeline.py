# ${{ values.pipelineName }}
# ${{ values.description }}
# Data Layer: ${{ values.layer }}

from pyspark.sql import SparkSession

def run_pipeline():
    """Main pipeline function for ${{ values.pipelineName }}"""
    spark = SparkSession.builder.appName("${{ values.pipelineName }}").getOrCreate()
    
    # TODO: Add your pipeline logic here
    print("Running ${{ values.pipelineName }} pipeline...")
    print("Layer: ${{ values.layer }}")

if __name__ == "__main__":
    run_pipeline()