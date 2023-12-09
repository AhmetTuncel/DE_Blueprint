import logging
import sys
from pyspark.sql import Window, DataFrame


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

