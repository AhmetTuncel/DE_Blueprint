import logging
import sys
from itertools import chain

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import when, col, create_map, lit, sha2, concat_ws
from pyspark.sql.types import IntegerType, StringType

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
        _df_stg_dim_domain_1 = self.table_data["stg_dim_domain_1"]

        logger.info(f"Initialize Data Sources")

        # Keeping id column for joining operations
        _df_stg_domains_combine = _df_stg_domain_1.withColumn("id", _df_stg_domain_2.id)

        # adding deperse column (needed for cust implementation)
        _df_stg_domains_combine = self.concealing_helper.execute(
            self.spark,
            _df_stg_domains_combine,
            column_names=["cust_id"],
        )

        logger.info("Finishing concealing")

        # Filtering combine table and drop col4
        _df_stg_domains_combine = _df_stg_domains_combine.filter(_df_stg_domains_combine.col_3 == "EN").drop("col_4")

        # Creating domains_combine DataFrames and drop col5 column
        _df_domains_combine = _df_stg_domains_combine.join(_df_stg_domain_3, "id", "left")
        _df_domains_combine = _df_domains_combine.drop("col_5")

        logger.info("Finishing concealing")

        return _df_domains_combine

    def add_assembly_attributes(_df_domains_combine: DataFrame, _df_stg_dim_domain_1: DataFrame) -> DataFrame:
        _df_domains_combine = _df_domains_combine.withColumn('col_5', when(
            (_df_domains_combine['col_6'].isNotNull() & (F.length('col_7') > 3)),
            F.split(F.col("col8"), "-")[0].cast(F.StringType())
        ).otherwise(F.lit(None).cast(F.StringType())))

        _df_domains_combine = (when(col('c_country') == 'BRA', '82.11')
                               .when((col('c_country') == 'DE'), '31.42')
                               .when((col('c_country') == 'TR'), '19.82')
                               .when((col('c_country') == 'USA'), '10.00')
                               .when((col('c_country') == 'UK'), '34.22')
                               .otherwise('').cast(StringType()))


        "Creating a dictionary named _df_stg_dim_domain_dic that maps the synthetic_col column"
        _df_stg_dim_domain_dic = {row['synthetic_col']: row['col_1'] for row in _df_stg_dim_domain_1.collect()}
        mapping_expr = create_map([lit(x) for x in chain(*_df_stg_dim_domain_dic.items())])

        _df_domains_combine = _df_domains_combine.withColumn('col_6',
                                                             mapping_expr[_df_domains_combine['col_7']].cast(
                                                                 StringType()))

        return _df_domains_combine


    @staticmethod
    def hash_colums_list() -> [str]:
        return ['col_8', 'col_9', 'col_10']





    @staticmethod
    def calculate_change(_df_stg_new: DataFrame, _df_stg_domain_current: DataFrame) -> DataFrame:

        _row_num_field = "row_now"

        _df_stg_new = _df_stg_new.withColumn("f_hash", sha2(concat_ws("",*DomainTable.hash_colums_list()), 256))
        _df_stg_new = _df_stg_new.withColumn("f_hash_last_change_timestamp", current_timestamp())
        # TODO: Implement current_timestamp function  