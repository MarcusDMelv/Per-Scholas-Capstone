from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkApp').getOrCreate()


# TODO: MUST Add json file to res folder
def load_json_data(json_file):
    df = spark.read.json('res/' + json_file + '.json')
    return df


# TODO: Use load_json_data('filename') to upload json files
def read_json_data():
    branch_data = load_json_data('cdw_sapp_branch')
    credit_data = load_json_data('cdw_sapp_credit')
    customer_data = load_json_data('cdw_sapp_custmer')
    return branch_data, credit_data, customer_data  # return tuple


# TODO: populate maria db -  create tables from imported data
def export_to_sql():
    # unpack tuple
    branch, credit, customer = read_json_data()  # adjust if adding new data
    # todo branch_data
    branch.write.format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:mysql://localhost:3307/creditcard_capstone") \
        .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
        .option("user", "root") \
        .option("password", "Pass1234") \
        .save()
    # todo credit_data
    credit.write.format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:mysql://localhost:3307/creditcard_capstone") \
        .option("dbtable", "creditcard_capstone.CDW_SAPP_CREDIT_CARD") \
        .option("user", "root") \
        .option("password", "Pass1234") \
        .save()
    # todo customer_data
    customer.write.format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:mysql://localhost:3307/creditcard_capstone") \
        .option("dbtable", "creditcard_capstone.CDW_SAPP_CUSTOMER") \
        .option("user", "root") \
        .option("password", "Pass1234") \
        .save()


# TODO Populate SQL database
# export_to_sql()  # uncomment to populate sql database

# TODO Load data from table in SQL database
def load_sql_table(df_name, table_name):
    spark = SparkSession.builder.appName('SparkApp').getOrCreate()
    df_name = spark.read.format("jdbc") \
        .option("url", "jdbc:mysql://localhost:3307/creditcard_capstone") \
        .option("dbtable", "creditcard_capstone." + table_name + "") \
        .option("user", "root") \
        .option("password", "Pass1234") \
        .load()
    return df_name


# TODO: Use load_sql_table(df_name,table_name)
def join_loaded_tables():
    branch = load_sql_table('branch_data', 'cdw_sapp_branch')
    credit_card = load_sql_table('credit_data', 'cdw_sapp_credit_card')
    customer = load_sql_table('branch_data', 'cdw_sapp_customer')
    # todo join dataframes
    '''joined_table = customer.join(credit_card, ["CREDIT_CARD_NO"]) \
        .join(branch, credit_card["BRANCH_CODE"] == branch["BRANCH_CODE"])'''
    joined_table = branch.join(credit_card,['BRANCH_CODE'])
    joined_table = joined_table.join(customer,['CREDIT_CARD_NO'])
    return joined_table


df = join_loaded_tables()
df.show(1)
