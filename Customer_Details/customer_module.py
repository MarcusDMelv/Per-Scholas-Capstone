from pyspark.sql.functions import regexp_replace

# todo load dataframe
from Credit_Card_Sys.credt_card_sys import df  # load df


# TODO 1) Used to check the existing account details of a customer.
def module_one():
    print('Module One:\n\tView existing account details of a customer by entering customer Social Security Number ('
          'SSN)\n')
    ccn = input('Enter customers credit card number: ')
    mod1 = df.select('SSN', 'FIRST_NAME', 'LAST_NAME', 'MIDDLE_NAME', 'CUST_COUNTRY', 'CUST_CITY', 'CUST_EMAIL',
                     'CUST_PHONE', 'STREET_NAME', 'CUST_STATE', 'CUST_ZIP', 'CREDIT_CARD_NO').filter(df.CREDIT_CARD_NO == ccn)
    mod1.distinct().show()


# TODO 2) Used to modify the existing account details of a customer.
def module_two():
    print('Module Two:\n\tView existing account details of a customer by entering customers credit card number\n')
    ccn = input('Enter customers credit card number: ')
    mod2 = df.select('SSN', 'FIRST_NAME', 'LAST_NAME', 'MIDDLE_NAME', 'CUST_COUNTRY', 'CUST_CITY', 'CUST_EMAIL',
                     'CUST_PHONE', 'STREET_NAME', 'CUST_STATE', 'CUST_ZIP', 'CREDIT_CARD_NO').filter(df.CREDIT_CARD_NO == ccn)
    mod2.distinct().show()
    # todo ask user what to modify
    column = input('Enter Column Name: ')
    replace = input('Enter current value from the column {}: '.format(column))
    new_entry = input('Enter new value: ')
    mod2.withColumn(column, regexp_replace(column, replace, new_entry)).distinct() \
        .show()


# TODO 3) Used to generate a monthly bill for a credit card number for a given month and year.
def module_three():
    print('Module Three:\n\tGenerate a monthly bill for a credit card number for a given month and year')
    ccn = input('Enter credit card number: ')
    month = input('Enter month: ')
    year = input('Enter year: ')
    mod3 = df.select('CREDIT_CARD_NO', 'FIRST_NAME', 'LAST_NAME', 'MONTH', 'YEAR', 'TRANSACTION_TYPE',
                     'TRANSACTION_VALUE').filter(
        df.CREDIT_CARD_NO == ccn). \
        filter(df.MONTH == month). \
        filter(df.YEAR == year)
    mod3.show()
    print('Credit Card Statement for Month:{} Year:{} '.format(month, year))
    mod3.agg({'TRANSACTION_VALUE': 'sum'}).show()


# TODO 4) Used to display the transactions made by a customer between two dates. Order by year, month, and day in
#  descending order
def module_four():
    print('Module Four:\n\tDisplay the transactions made by a customer between two dates.')
    ccn = input('Enter customers credit card number: ')
    start_day = input('Enter START Day: ')
    start_month = input('Enter START Month: ')
    start_year = input('Enter START Year: ')
    end_day = input('Enter End Day: ')
    end_month = input('Enter END Month: ')
    end_year = input('Enter END Year: ')

    mod4 = df.select('CREDIT_CARD_NO','YEAR', 'MONTH', 'DAY', 'TRANSACTION_TYPE', 'TRANSACTION_VALUE'). \
        filter(df.CREDIT_CARD_NO == ccn). \
        filter(df.YEAR >= start_year). \
        filter(df.MONTH >= start_month). \
        filter(df.DAY >= start_day). \
        filter(df.YEAR <= end_year). \
        filter(df.MONTH <= end_month). \
        filter(df.DAY <= end_day). \
        sort(df.YEAR.desc(),df.MONTH.desc(),df.DAY.desc())
    mod4.show(100)


module_one()
module_two()
module_three()
module_four()