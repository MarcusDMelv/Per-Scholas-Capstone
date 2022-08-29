import pandas as pd
from matplotlib import pyplot as plt

# TODO: show all columns
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# TODO: import pandas df
from Loan_App_Data.loan_app_data import pandas_df

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
    print(new_df.head(20))
    new_df.plot(kind='bar', title='App approvals: Married Men vs Married Women based on income ranges.'
                , ylabel='Application Approved', xlabel='Gender/Income Range', figsize=(12, 12))
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
    print(new_df.head(20))
    new_df.plot(kind='bar', title='App Approvals: difference in application approvals based on Property Area'
                , ylabel='Application Approved', xlabel='Property_Area', figsize=(12, 12))
    plt.show()

    # Show Plot

# TODO 3) Create a multi-bar plot that shows the total number of approved applications per each application demographic.
def module_three():
    print(
        'Module Three:\n\tCreate a multi-bar plot that shows the total number of approved applications per each '
        'application demographic.\n')
    new_df = df[['Property_Area', 'Application_Status']]
    # Query by one conditions
    new_df = new_df.query("Application_Status == 'Y' ")
    new_df = new_df.groupby(['Property_Area'])['Application_Status'].count()
    print(new_df.head(20))
    new_df.plot(kind='bar', title='App Approvals: difference in application approvals based on Property Area'
                , ylabel='Application Approved', xlabel='Property_Area', figsize=(12, 12))
    plt.show()

    # Show Plot


module_one()
module_two()
