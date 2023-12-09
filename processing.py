import logging
import sys
from pyspark.sql import DataFrame, functions as F


logger = logging.getLogger()
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levename)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class StagingTable:

    def __init_(self, table_def: [str]):
        self.table_def = table_def
        self.table_def = {}

    def set_data(self, name: str, df: DataFrame):
        self.table_data = df

    def join_data(self) -> DataFrame:
        pass


class DomainTable(StagingTable):

    def __init_(self, table_def: [str], concealing_helper, spark, table_setting):
        super().__init_(table_def)
        self.concealing_helper = concealing_helper
        self.spark = spark
        self.table_setting = table_setting

    def join_data(self) -> DataFrame:
        logger.info(f"Join domain Info with {self.table_def}")
        self.pseudonymizer_helper.initialize_pseudonymizer(self.table_def, self.spark)

        _df_stg_domain_1 = self.table_data["stg_domain_1"]
        _df_stg_domain_2 = self.table_data["stg_domain_2"]
        _df_stg_domain_3 = self.table_data["stg_domain_3"]
        _df_stg_domain_4 = self.table_data["stg_domain_4"]


        logger.info(f"Initialize Data Sources")

        #Keeping id column for joining operations
        _df_stg_domains_combine = _df_stg_domain_1.withColumn("id", _df_stg_domain_2.id)

        #adding deperse column (needed for cust implementation)
        _df_stg_domains_combine = self.concealing_helper.execute(
            self.spark,
            _df_stg_domains_combine,
            column_names=["cust_id"],
        )

        logger.info("Finishing concealing")

        # Filtering combine table and drop col4
        _df_stg_domains_combine = _df_stg_domains_combine.filter(_df_stg_domains_combine.col3 == "EN").drop("col4")

        # Creating domains_combine DataFrames and drop col5 column
        _df_domains_combine = _df_stg_domains_combine.join(_df_stg_domain_3, "id", "left")
        _df_domains_combine = _df_domains_combine.drop("col5")

        logger.info("Finishing concealing")

        return _df_domains_combine


    def add_assembly_attributes(_df_domains_combine: DataFrame, _df_stg_domain_4: DataFrame) -> DataFrame:

        _df_domains_combine = _df_domains_combine.withColumn('col5', when(
            (_df_domains_combine['col6'].isNotNull() & (F.length('col7') > 3)),
            F.split(F.col("col8"), "-")[0].cast(F.StringType())
        ).otherwise(F.lit(None).cast(F.StringType())))



