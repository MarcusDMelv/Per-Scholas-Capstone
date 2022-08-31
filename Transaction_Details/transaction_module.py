# todo load dataframe
from Credit_Card_Sys.credt_card_sys import df  # load df


# todo 1) Used to display the transactions made by customers living in a given zip code for a given month and year.
#  Order by day in descending order.
def module_one():
    print('Module One:\n\tView transactions made by customers living in a given zip code for a given month and year:\n')
    zipcode = input('\tEnter Zipcode: ')
    year = input('\tEnter Year: ')
    month = input('\tEnter Month: ')
    mod1 = df.select('FIRST_NAME','LAST_NAME','CUST_ZIP', 'MONTH', 'DAY', 'YEAR', 'TRANSACTION_ID', 'TRANSACTION_TYPE', 'TRANSACTION_VALUE').distinct(). \
        filter(df.MONTH == month). \
        filter(df.YEAR == year). \
        filter(df.CUST_ZIP == zipcode). \
        sort(df['DAY'].desc())
    mod1.show()


# TODO 2) Used to display the number and total values of transactions for a given type.
def module_two():
    print('Module Two:\n\tView the number and total values of transactions for a given type\n')
    tran_type = input('\tEnter type of transaction \n\t\tChoose from: Education, Entertainment, Healthcare, '
                      'Grocery, Test, Gas, Bills: ')
    tran_type = tran_type.capitalize()
    mod2 = df.select('TRANSACTION_TYPE', 'TRANSACTION_VALUE').filter(df.TRANSACTION_TYPE == tran_type)
    print("\nTotal number of {} transactions in Dataframe = {}".format(tran_type, mod2.count()))
    print("\nTotal value of {} transactions in Dataframe".format(tran_type))
    mod2.agg({'TRANSACTION_VALUE': 'sum'}).show()


# TODO 3) Used to display the number and total values of transactions for branches in a given state.
def module_three():
    print('Module Three:\n\tView the number and total values of transactions for branches in a given state.\n')
    state = input('\tEnter state abbreviation: ')
    state = state.upper()
    mod3 = df.select('BRANCH_STATE', 'TRANSACTION_TYPE', 'TRANSACTION_VALUE').filter(df.BRANCH_STATE == state)
    print("\nTotal number of branch transactions in {} = {} ".format(state, mod3.count()))
    print("\nTotal value of branch transactions in {}".format(state))
    mod3.agg({'TRANSACTION_VALUE': 'sum'}).show()

def run_transaction_module():
    module_one()
    module_two()
    module_three()
