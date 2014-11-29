__author__ = 'andmas'

from subprocess import call
import os

epoch_results_dir = "/mnt/4tbdata/test_epoch_results"

def epoch_search(main_path,results_path):
    results = os.listdir(main_path)
    for dir_list in results:

        dir_string = str(dir_list)
        dir_list = dir_string.strip('\'(),')
        move_to_dir = main_path + "/" + dir_list
        sub_dir = os.listdir(move_to_dir)
        for subs in sub_dir:
            # if subs == 'processed':
            #     sub_dir_name = main_path + "/" + dir_list + "/" + subs
            #     sub_sub_dir = os.listdir(sub_dir_name)
            #     for subs_subs in sub_sub_dir:
                    if subs == 'test':
                        print "subs : " + str(subs)
                        path_for_test = main_path + "/" + str(dir_list) + "/" + str(subs)
                        file_listing = os.listdir(path_for_test)
                        lc_files=[]
                        for files in file_listing:
                            gti_front_filename, gti_filename_ext = os.path.splitext(files)
                            gti_file = gti_front_filename[0:2]

                            if gti_filename_ext == '.txt':
                                text_gti = gti_front_filename + gti_filename_ext


                            lc_front_filename, lc_filename_ext = os.path.splitext(files)
                            detid = lc_front_filename.split("_")
                            size_of_list = len(detid)
                            if size_of_list > 2:
                                convert_detid = detid[2]
                                if convert_detid == 'corrected':
                                    lc_files.append(files)


                        for items in lc_files:
                            split_filename, split_ext = os.path.splitext(items)
                            split_up = split_filename.split("_")
                            pass_obs_id = split_up[0]
                            pass_detid = split_up[1]
                            split_energy_range = split_up[4]
                            saved_energy_range = split_energy_range[0:4]
                            if saved_energy_range == '1000':
                                invoke_period_string = "cd " + path_for_test + " && idl -e chimax2_v2 -args " \
                                                       + path_for_test + "/" + str(items) + " " + path_for_test \
                                                       + "/" + text_gti + " " + epoch_results_dir + " " + pass_obs_id \
                                                       + " " + "1 15" + " " + "0.2 1500" + " " + pass_detid
                                print invoke_period_string
                                call(invoke_period_string, shell=True, executable='/bin/bash')
                                print "passed idl chi-squared call"


    print "########################## Finished #########################################################"
    print "#############################################################################################"
    print "#############################################################################################"
    print "#############################################################################################"
    print "#############################################################################################"
    print "#############################################################################################"
    print "#############################################################################################"


if __name__ == '__main__':
    epoch_search(main_path,results_path)