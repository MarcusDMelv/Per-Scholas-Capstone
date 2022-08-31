import matplotlib.pyplot as plt
# todo load dataframe
from Credit_Card_Sys.credt_card_sys import df  # load df

df = df
# pandas_df = df.toPandas()


# TODO 1) Find and plot transactions, showing which transaction type occurs most often.
def module_one():
    print('Module One:\n\tFind and plot transactions, showing which transaction type occurs most often.\n')
    mod1 = df.select('TRANSACTION_TYPE')
    pandas_df = mod1.toPandas()
    plt.rcParams["figure.figsize"] = [8, 4]
    plt.rcParams["figure.autolayout"] = True
    plt.plot(pandas_df['TRANSACTION_TYPE'].value_counts(),ls='-.', c='r', marker='o')
    plt.title('Which Transaction Occurs Most Often')
    plt.ylabel('Number of Transaction')
    plt.xlabel('Transaction Type')
    plt.text(0, 6860, 'Bills occurs most often')  # see note below
    plt.show()


# TODO 2) Find and plot states, showing which state has the highest number of customers.
def module_two():

    print('Module Two:\n\tFind and plot states, showing which state has the highest number of customers.\n')
    mod2 = df.select('CUST_STATE')
    pandas_df = mod2.toPandas()
    plt.rcParams["figure.figsize"] = [15, 5]
    plt.rcParams["figure.autolayout"] = False
    plt.plot(pandas_df['CUST_STATE'].value_counts(), ls='-', c='g', marker='o')
    plt.title('Which State Has Highest Number Of Customers')
    plt.ylabel('Number of Customers')
    plt.xlabel('State Abbrev.')
    plt.text(0, 4800, 'NY has the most customers')  # see note below
    plt.show()


# TODO 3) Find and plot the sum of all transactions for each customer, and which customer has the highest transaction
#  amount. (First 20) hint(use CUST_SSN).
def module_three():
    print('Module Three:\n\tFind and plot the sum of all transactions for each customer, and which customer has the '
          'highest transaction amount. (First 20) hint(use CUST_SSN).\n')
    mod3 = df.select('CUST_EMAIL', 'FIRST_NAME', 'LAST_NAME', 'TRANSACTION_VALUE')
    pandas_df = mod3.toPandas()
    pandas_df = pandas_df.groupby('CUST_EMAIL')['TRANSACTION_VALUE'].sum().reset_index()
    pandas_df = pandas_df.sort_values(by=['TRANSACTION_VALUE'], ascending=False)
    pandas_df = pandas_df[:20]
    plt.rcParams["figure.figsize"] = [35, 5]
    plt.rcParams["figure.autolayout"] = True
    plt.plot(pandas_df['CUST_EMAIL'], pandas_df['TRANSACTION_VALUE'],ls=':', c='limegreen', marker='o')
    plt.xticks(pandas_df['CUST_EMAIL'])
    plt.title('Which Customer Has The Highest Transaction Amount.')
    plt.ylabel('Transaction Amount')
    plt.xlabel('Customer Email')
    plt.text(0, 7621, 'Phil Maurice Moore has the highest transaction amount: $ 7620.30')  # see note below
    plt.show()


# TODO 4) Find and plot the top three months with the largest transaction data.
def module_four():
    print('Module Four:\n\tFind and plot the top three months with the largest transaction data.\n')
    mod4 = df.select('MONTH', 'TRANSACTION_VALUE')
    pandas_df = mod4.toPandas()
    pandas_df = pandas_df.groupby('MONTH')['TRANSACTION_VALUE'].sum().reset_index()
    pandas_df = pandas_df.sort_values(by=['TRANSACTION_VALUE'], ascending=False)
    pandas_df = pandas_df.sort_values(by=['MONTH'], ascending=False)
    pandas_df = pandas_df[:3]
    plt.rcParams["figure.figsize"] = [15, 5]
    plt.rcParams["figure.autolayout"] = True
    plt.plot(pandas_df['MONTH'], pandas_df['TRANSACTION_VALUE'],ls='-', c='g',lw='3', marker='o')
    plt.title('Top Three Months With Highest Number Of Transactions')
    plt.ylabel('Transaction Amount')
    plt.xlabel('Month by Number')
    plt.text(10, 202584, 'October has the most transactions: $202583.89')  # see note below
    plt.show()


# TODO 5) Find and plot each branches healthcare transactions, showing which branch  processed the highest total
#  dollar value of healthcare transactions.
def module_five():
    print('Module Five:\n\tFind and plot each branches healthcare transactions, showing which branch  processed the '
          'highest total dollar value of healthcare transactions.\n')
    mod5 = df.select('BRANCH_CODE', 'TRANSACTION_VALUE').filter(df.TRANSACTION_TYPE == 'Healthcare')
    pandas_df = mod5.toPandas()
    pandas_df = pandas_df.groupby('BRANCH_CODE')['TRANSACTION_VALUE'].sum().reset_index()
    pandas_df = pandas_df.sort_values(by=['TRANSACTION_VALUE'], ascending=False)
    pandas_df = pandas_df[:10]
    pandas_df = pandas_df.sort_values(by=['BRANCH_CODE'], ascending=True)
    print(pandas_df)
    plt.rcParams["figure.figsize"] = [15, 5]
    plt.rcParams["figure.autolayout"] = True
    plt.plot(pandas_df['BRANCH_CODE'], pandas_df['TRANSACTION_VALUE'],ls='-', c='purple', marker='o')
    plt.xticks(pandas_df['BRANCH_CODE'])
    plt.title('Which Branch Has Highest Number Of Healthcare Transactions')
    plt.ylabel('Transaction Amount')
    plt.xlabel('Branch Code')
    plt.text(25, 4371, 'Branch code: 25 has the most Healthcare transactions: $ 4370.18')  # see note below
    plt.show()

# export
def run_dav_module():
    module_one()
    module_two()
    module_three()
    module_four()
    module_five()
