from Customer_Details.customer_module import run_customer_module
from Data_Analysis_Visualization.analysis_visualization_module import run_dav_module
from Transaction_Details.transaction_module import run_transaction_module
from Loan_App_DAV.loan_app_dav import run_loan_app_dav_module
import time

# TODO grab imported functions
# customer module
customer_mod = run_customer_module
# dav module
dav_module = run_dav_module
# transaction module
trans_module = run_transaction_module
# loan app dav
loan_app_dav = run_loan_app_dav_module


# TODO Construct program flow
def run_program():
    # run customer module
    print('Running Customer Module:')
    customer_mod()
    time.sleep(2)
    # run dav module
    print('Running Data Analysis Visualization Module:')
    dav_module()
    time.sleep(2)
    # run transactions module
    print('Running Transaction Module:')
    trans_module()
    time.sleep(2)
    # run loan app dav module
    print('Running Load App DV Module:')
    loan_app_dav()
    print('End of program. Thank you')


def main():
    run_program()


if __name__ == "__main__":
    main()
