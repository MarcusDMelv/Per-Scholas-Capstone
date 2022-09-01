import pandas as pd
from matplotlib import pyplot as plt

# import loan app pandas df
from Loan_App_Data.loan_app_data import pandas_df

# TODO: show all columns
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# load loan app pandas df
df = pandas_df
print(df.head(2))


# TODO 1) Create a bar chart that shows the difference in application approvals for Married Men vs Married Women
#  based on income ranges. (number of approvals)
def module_one():
    print(
        'Module One:\n\tCreate a bar chart that shows the difference in application approvals for Married Men vs '
        'Married Women based on income ranges. (number of approvals)\n')
    new_df = df[['Gender', 'Married', 'Application_Status', 'Income']]
    # Query by multiple conditions
    new_df = new_df.query("Married == 'Yes' and Application_Status == 'Y' ")
    new_df = new_df.groupby(['Income', 'Gender'])['Application_Status'].count()
    print(new_df)
    new_df.plot(kind='bar', title='App Approvals: Married Men vs Married Women| Based on income ranges.'
                , ylabel='Application Approved', xlabel='Income Range/Gender', figsize=(12, 12), color='purple')
    plt.show()


# TODO 2) Create and plot a chart that shows the difference in application approvals based on Property Area.
def module_two():
    print(
        'Module Two:\n\tCreate and plot a chart that shows the difference in application approvals based on Property '
        'Area.\n')
    new_df = df[['Property_Area', 'Application_Status']]
    # Query by one conditions
    new_df = new_df.query("Application_Status == 'Y' ")
    new_df = new_df.groupby(['Property_Area'])['Application_Status'].count()
    print(new_df)
    new_df.plot(kind='bar', title='App Approvals: Difference in application approvals| Based on Property Area'
                , ylabel='Application Approved', xlabel='Property_Area', figsize=(12, 12), color='orange')
    plt.show()

    # Show Plot


# TODO 3) Create a multi-bar plot that shows the total number of approved applications per each application demographic.
def module_three():
    print(
        'Module Three:\n\tCreate a multi-bar plot that shows the total number of approved applications per each '
        'application demographic.\n')
    data = df[['Income', 'Application_Status', 'Gender', 'Education']]
    # edit inputs
    # Query by one condition
    data = data.query("Application_Status == 'Y' ")
    data = data.groupby(['Income', 'Gender', 'Education'])['Application_Status'].count()
    print(data)
    data.plot(kind='bar', title='App Approvals: Number of approved applications | Per each application demographic'
              , ylabel='Applications Approved', xlabel='Demographic:Income Range / Gender/ College', figsize=(12, 12),
              color='green')
    plt.show()

    # Show Plot


# export to main.py
def run_loan_app_dav_module():
    module_one()
    module_two()
    module_three()
module_one()
module_two()
module_three()