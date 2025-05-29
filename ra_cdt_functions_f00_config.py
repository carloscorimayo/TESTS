# ra_cdt_functions_f00_config.py

import pandas as pd
import numpy as np
from dss_airflow_utils.workspace_utils import path_in_workspace, get_workspace
#-------------------------------------------------------------------#
#   Global Functions
#-------------------------------------------------------------------#

#-------------------------------------------------------------------#
#   get_config(context)
#   'TEST_OR_NOTEBOOK' or parameter for airflow
#   Depending on if doing a (test and Notebook are combined as 'TEST_OR_NOTEBOOK'),
#   or Airflow is False
#-------------------------------------------------------------------#
def get_config(context):
    #SH: 20200222: UnComment for airflow, Comment out for Notebook
    # need this to read in user parameters from job_request

    if not context == 'TEST_OR_NOTEBOOK':
        #Normal airflow run.
        thepath = get_workspace()
        print("thepath: ", thepath)
        if type(context) != type(str()):
            print("context['jr'] is: ", context['jr'])
            print("context is not a string!")
            df = pd.DataFrame(context['jr'])
            df.to_csv("{}/jobrequest.csv".format(get_workspace()), index=False)

        class get_config:
            # By Sherrie Huey: 20200222
            # Imitates the dss_airflow_utils.utils get_config for context

            def __init__(self, context):
                #For Airflow to make up for dss_airflow_utils.utils.get_config
                print("context['jr'] is: ", context['jr'])
                df = pd.DataFrame(context['jr'])
                df.to_csv("{}/jobrequest.csv".format(get_workspace()), index=False)

            @staticmethod
            def get(context):
                jobrequest_file = path_in_workspace('jobrequest.csv')
                jobrequest = pd.read_csv(jobrequest_file, sep=',')
                #20210624:SH: Odd Error for DG: \
                #KeyError: 'DG_BFF_file_FromAzureBlobStorage'
                if context == 'DG_BFF_file_FromAzureBlobStorage':
                    returnit = jobrequest.get('DG_BFF_file_FromAzureBlobStorage', 'DG_BFFs.csv')
                    print('Inside if and returning: ', returnit)
                    if type(returnit) == str:
                        print("Odd situation where did not actually find:")
                        print("DG_BFF_file_FromAzureBlobStorage")
                        print("Using instead default of: DG_BFFs.csv")
                        print("which is hard-coded - plan accordingly.")
                        return returnit
                    else:
                        return returnit.item()
                elif type(jobrequest[context][0]) == np.bool_:
                    #jobrequest[context][0]:
                    #jobrequest.get(context):
                    print("type as np.bool_")
                    if jobrequest[context][0]:
                        print("Returning true")
                        return "true"
                    else:
                        print("Returning false")
                        return "false"
                else:
                    returnit = jobrequest.get(context)
                    #print("Returning: ", returnit.item())
                    #return returnit.item()    #jobrequest[context][0]
                    #20221019:SH: code acting funny
                    return jobrequest[context][0]
    else:
        #Is a notebook or PYTest run
        import yaml
        class get_config:
            # By Sherrie Huey: 20200222
            # Imitates the dss_airflow_utils.utils get_config for context
            # c = get_config(context) # where context can be ''
            # do_element_group = c.get('do_element_group')
            def __init__(self, context):
                print("context: ", context)
                with open("PYTEST_job_request.yaml", "r") as ymlfile:
                    cfg = yaml.load(ymlfile)

                self = cfg

            @staticmethod
            def get(context):
                print("In get(context) for test or Notebook: ", context)

                with open("PYTEST_job_request.yaml", "r") as ymlfile:
                    cfg = yaml.load(ymlfile)
                for key in cfg["configuration"]:
                    if key == context:
                        print("parameter is: ", context)
                        print("value : ", cfg["configuration"][context])
                        if type(cfg["configuration"][context]) == np.bool_:
                            if cfg["configuration"][context]:
                                return "true"
                            else:
                                return "false"
                        else:
                            return cfg["configuration"][context]

    return get_config
