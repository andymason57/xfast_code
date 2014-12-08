__author__ = 'andmas'
from subprocess import call
import os

#global variables
#fasper_base_dir = "/mnt/4tbdata/"


def fasper(main_path, results_path):
    print "hit FASPER!!!!"
    print "main results FASPER path passed: " + main_path
    print "path where FASPER results stored: " + results_path
    results = os.listdir(main_path)

    for dir_list in results:
        print "dir_list: " + str(dir_list)
        #pull_obs.execute("DELETE FROM not_reduced WHERE obs_id = %s", (dir_list,))
        dir_string = str(dir_list)
        dir_list = dir_string.strip('\'(),')
        move_to_dir = main_path + "/" + dir_list
        sub_dir = os.listdir(move_to_dir)
        for subs in sub_dir:
            # if subs == 'processed':
            #     print "hit processed directory in fasper"
            #     sub_dir_name = main_path + "/" + dir_list + "/" + subs
            #     sub_sub_dir = os.listdir(sub_dir_name)
            #     for subs_subs in sub_sub_dir:
                    if subs == 'test':
                        print "subs : " + str(subs)
                        path_for_test = main_path + "/" + str(dir_list) + "/" + str(subs)
                        print path_for_test

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
                            pass_detid = split_up[1]
                            split_energy_range = split_up[4]
                            saved_energy_range = split_energy_range[0:4]
                            if saved_energy_range == '1000':
                                invoke_period_string = "cd " + path_for_test + " && idl -e FASPER_v2 -args " \
                                                       + path_for_test + "/" + items + " " + path_for_test + "/" \
                                                       + text_gti + " " + results_path + "/" + " " + dir_list + " " \
                                                       + "1 1" + " " + saved_energy_range + " " + pass_detid
                                print invoke_period_string
                                call(invoke_period_string, shell=True, executable='/bin/bash')
                                print "passed idl call"

                        #return results_path

    print "########################## Finished #########################################################"
    print "#############################################################################################"
    print "#############################################################################################"
    print "#############################################################################################"
    print "#############################################################################################"
    print "#############################################################################################"
    print "#############################################################################################"


def plot_results(results_path):
    # call fasper routine
    plot_results_string = "cd " + results_path + " && idl -e plot_fasper_results -args " \
                          + results_path + "/"
    print plot_results_string
    call(plot_results_string, shell=True, executable='/bin/bash')


#if __name__ == '__main__':
#    fasper(main_path,results_path)
 #   plot_results(results_path)