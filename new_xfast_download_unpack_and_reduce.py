from subprocess import call
import os
import sys
import shutil
import logging

import psycopg2


# ####### Main script to automate xfast data download, extraction and processing with SAS. ############################
""" Python script to automate reduction of XMM-Newton observations with the pn detector.
Extracts each source from each observation within the 3XMM-DR4 catalog that has a lightcurve and spectrum (~ 120,000).
 Reads observations to process from Postgres table, calls XMM SAS tasks, creates GTI based on pre-processed flare
  lightcurves, background subtracts data and then initiates a Lomb-Scargle and epoch folding based timing analysis of
  the frame time extract lighhtcurve.
"""

# import script to run FASPER Lomb-Scargle routine.
from timing_analysis_codes import new_call_to_lomb_scargle_FASPER

# import script to run epoch folding routine
from timing_analysis_codes import call_to_epoch_search


# TODO put in code to deal with clean/unclean obs/sources - IDL.

""" globals to use for location of data and results - path to download, LS and epoch results and lrzipped directories """
base_dir = "/mnt/4tbdata/test_both_data"
ls_results_dir = "/mnt/4tbdata/test_both_results"
epoch_results_dir = "/mnt/4tbdata/test_both_epoch"
lrztar_output_dir = "/mnt/4tbdata/test_both_tarred"


################################ Set up error logging facility ########################################################

#set logger file destination 
logging.basicConfig(filename=base_dir + '/reduced_data_errors.log')
#set level of logging - debug - lowest level everything logged
logging.basicConfig(level=logging.DEBUG)
#create logger and logging facility
logger = logging.getLogger('__name__')
#create logging handler
hdlr = logging.FileHandler(base_dir + '/reduced_data_errors.log')
# format logging time and level 
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
#add handler to logger
hdlr.setFormatter(formatter)
logger.addHandler(hdlr)
#logger.setLevel(logging.DEBUG)
#format logging messages


#############################  Retrieve all obsIDs from Postgres to process ###########################################
""" Retrieve list of Obs IDs to process from Postgres DB

    Populates list of observation IDs to process by retrieving data from postgres table

    :param none
    :returns: A populated list of Observational IDs

"""

def fetch_all_obsIDs():
    try:
        conn = psycopg2.connect("dbname=postgres user=postgres host=127.0.0.1 password=YvonneCSutton42")
        #open cursor
        fetch_data = conn.cursor()
        #select unique obs_ids from table all_unique
        fetch_data.execute("SELECT obs_id FROM test_cases_two;")
        #commit execute statement
        conn.commit()

        #Fetch all obs_ids from all_unique Table
        ids = fetch_data.fetchall()
        #close cursor
        fetch_data.close()
        conn.close()
        return ids

    except psycopg2.DatabaseError:
        print "I am unable to connect to the database"
        logger.exception('I am unable to connect to the database')
        if conn:
            conn.rollback()
            print 'Error %s'
            sys.exit(1)


########################## Convert ObsIDs and insert into new table  ##################################################
""" converts observational IDs to format as ten digit integers

    :param ids: The list of all Observation IDs to process
    :returns built_up_list: A 10 digit format list of Observational IDs
"""

def convert_and_insert_obsIDs(ids):
    #initialise empty list 
    built_up_list = []

    #Need to check if obsIDs are 10 digits in length if not append 0 to front and write back to new table in database.
    for row in ids:
        try:
            #need to convert list elements into strings
            str_row = str(row)
            #strip of leading/ending ( and ) and , chars
            striped_row = str_row.strip('\'(),')
            if len(striped_row) == 10:
                #append stripped obsID as integer into new list 'built_up_list'
                built_up_list.append(striped_row)
            elif len(striped_row) == 9:
                striped_row = "0" + striped_row
                built_up_list.append(striped_row)
            elif len(striped_row) == 8:
                striped_row = "00" + striped_row
                built_up_list.append(striped_row)
            elif len(striped_row) == 7:
                striped_row = "000" + striped_row
                built_up_list.append(striped_row)
        except RuntimeError:
            logger.exception('Error in def. convert_and_insert_obsIDs in appending zeros to string ID ' + str_row)
    return built_up_list


################################## Main part of script - Download data, reduce, compress and store results #############
"""
    Downloads data and decompresses. Calls IDL to initiate SAS routines. First fetch list of obsIDs to reduce from
    Postgres table. ObsID once fetched is removed from table. Need to have a copy of aioclient and lib dir.
    in download directory. Then sends request to XSA to download raw (odf), pipeline processed (pps) for that
    observation.
    Decompresses download tar file using python routine from
    http://guanidene.blogspot.fi/2011/06/nested-tar-archives-extractor.html as heavily nested.

    Make sure extractnested.py is included in folder within PATH.
    Also set permissions chmod ugo+rx extractnested.py
    The Call string uses path to extractnested.py script and also needs executable='/bin/bash'
    otherwise /bin/sh shell is called by default.

    Copies downloaded and extracted data into a new directory structure odf (raw), pps(pipeline processed)
    and creates a new directory processed with subdir test to hold results of processing.

    Then calls main IDL processing script xmmpipeworking to process that observation.
    After this is completed removes aioclient and lib dir as tey are no longer required.

    Then calls the Lomb-Scargle IDL routine to produce periodogram.
    Followed by call to IDL routine to perform epoch folding search.

    Source and background spectra produced to determine BACKSCAL are then removed to decrease results dir size.
    Finally run lrzip to compress products of reduction by IDL and then delete the uncompressed results directories.


    :param built_up_list: Constructed list of observational IDs
    """

def download_data(built_up_list):
    for obs_id in built_up_list:
        try:
            # pull obs_id of stack
            conn = psycopg2.connect("dbname=postgres user=postgres host=127.0.0.1 password=YvonneCSutton42")
            #open cursor
            cur = conn.cursor()
            cur.execute("INSERT INTO test_cases_two_results (obs_id) VALUES (%s)", (obs_id,))
            cur.execute("DELETE FROM test_cases_two WHERE obs_id = %s", (obs_id,))
            conn.commit()
            conn.close()

        except psycopg2.DatabaseError:
            print "Database exception error... rolling back"
            logger.exception('Database exception error ... rolling back')
            if conn:
                conn.rollback()
                print 'Database Error %s'

        try:
            create_obsID_dir = "cd " + base_dir + " && mkdir " + obs_id
            print create_obsID_dir
            call(create_obsID_dir, shell=True, executable='/bin/bash')
            #copy aioclient scripts to download directory
            copy_aioclient = "cd " + base_dir + " && cp -ar " + base_dir + "/lib " + base_dir + "/" + obs_id + " && " + \
                             "cp -ar " + base_dir + "/aioclient " + base_dir + "/" + obs_id + " && chmod u+x aioclient"
            print copy_aioclient

            call(copy_aioclient, shell=True, executable='/bin/bash')
            #call aioclient script to download data in obs_id directory
            dir_str = "cd " + base_dir + "/" + obs_id + " && ./aioclient -L " + '"GET obsno=' + obs_id + " -O " + obs_id + '"'

            #call to shell to execute aioclient with neccessary parameters
            call(dir_str, shell=True, executable='/bin/bash')

            ###################################### Untar download files #############################################################################
            # untar downloaded files from XSA.
            # uses python routine from http://guanidene.blogspot.fi/2011/06/nested-tar-archives-extractor.html
            # Make sure extractnested.py is included in folder within PATH.
            #Also set permissions chmod ugo+rx extractnested.py
            #the Call string uses path to extractnested.py script and also needs executable='/bin/bash'
            #otherwise /bin/sh shell is called by default

            #have to rename downloaded .tar file from GUEST**** to obs_id.tar
            rename_downloaded_tarfile = "cd " + base_dir + "/" + obs_id + " && " + "for i in ./*GUEST*;do mv -- " + '"$i"' + " " + '"' + obs_id + '.tar' + '"' + ";done"
            call(rename_downloaded_tarfile, shell=True, executable='/bin/bash')
            unpack_str = base_dir + '/extractnested.py' + " " + base_dir + "/" + obs_id + "/" + obs_id + ".tar"
            call(unpack_str, shell=True, executable='/bin/bash')

            ################################### Move files into required directories ###################################

            #set up new directory structure in download directory
            move_to_download_dir = "cd " + base_dir + "/" + obs_id + " && mkdir odf && mkdir pps && mkdir processed"
            print move_to_download_dir
            call(move_to_download_dir, shell=True, executable='/bin/bash')
            set_up_subdir = "cd " + base_dir + "/" + obs_id + "/processed && mkdir test"
            call(set_up_subdir, shell=True, executable='/bin/bash')
            #Copy odf and pps files into new directories.
            src = base_dir + "/" + obs_id + "/" + obs_id + "/" + obs_id + "/odf" + "/" + obs_id + "/"
            odf_dest = base_dir + "/" + obs_id + "/odf/"
            #Copy files from downloaded odf dir. to new odf dir. If file is actually directory obtain the name of it.
            src_files = os.listdir(src)
            for file_name in src_files:
                full_file_name = os.path.join(src, file_name)
                if os.path.isfile(full_file_name):
                    shutil.copy(full_file_name, odf_dest)
                elif os.path.isdir(full_file_name):
                    sub_dir = os.path.join(src, file_name)
            #list files in downloaded odf sub dir. and copy to new odf dir.
            src_files = os.listdir(sub_dir)
            for file_name in src_files:
                full_file_name = os.path.join(sub_dir, file_name)
                if os.path.isfile(full_file_name):
                    shutil.copy(full_file_name, odf_dest)
            #initialise path of main downloaded pps dir and path of new pps dir.
            src_pps = base_dir + "/" + obs_id + "/" + obs_id + "/" + obs_id + "/pps"
            pps_dest = base_dir + "/" + obs_id + "/pps/"
            #Copy files from downloaded pps dir. to new pps dir.
            src_files = os.listdir(src_pps)
            for file_name in src_files:
                full_file_name = os.path.join(src_pps, file_name)
                if os.path.isfile(full_file_name):
                    shutil.copy(full_file_name, pps_dest)
            #convert .FTZ files in pps dir to .fits files
            src_files_ftz = os.listdir(pps_dest)
            for file_name in src_files_ftz:
                front_filename, file_extension = os.path.splitext(file_name)
                if file_extension == '.FTZ':
                    full_file_name = pps_dest + "/" + front_filename + file_extension
                    change_ext = "mv " + full_file_name + " " + pps_dest + "/" + front_filename + ".fit.gz"
                    call(change_ext, shell=True, executable='/bin/bash')
                    call_gzip = "gunzip " + pps_dest + "/" + front_filename + ".fit.gz"
                    call(call_gzip, shell=True, executable='/bin/bash')
                    #copy ccf.cif file into odf directory
            copy_ccf_file = "cp " + base_dir + "/ccf.cif " + odf_dest
            call(copy_ccf_file, shell=True, executable='/bin/bash')
            #remove untarred download directory
            remove_untarred = "rm -R " + base_dir + "/" + obs_id + "/" + obs_id
            print remove_untarred
            call(remove_untarred, shell=True, executable='/bin/bash')
        except IOError:
            logger.exception('Error: can\'t find file or read data')
            logger.exception('Error occured in moving files in DOWNLOAD_DATA def... ' + ' the obsID is ', obs_id)


        ######################################### Call Main IDL reduction scripts ####################################
        # Call to IDL xmmpipeworking script to reduce data - need to pass path to files

        try:
            idl_call = "cd " + base_dir + " && idl -e xmmpipeworkingv30 -args " + " " + base_dir + "/" + obs_id + " " + obs_id
            call(idl_call, shell=True, executable='/bin/bash')
        except RuntimeError:
            logger.exception(
                'Some runtime error has occurred in main IDL reduction routine - check IDL console logs for ObsID: ',
                obs_id)

        ######################################## remove files so doesn't throw an error ##############################
        # removes aioclient and lib directories so exception is not raised
        try:
            remove_files = "cd " +  base_dir + "/" + obs_id + " && rm aioclient " + " && rm " + obs_id + ".tar"
            call(remove_files, shell=True, executable='/bin/bash')
        except RuntimeError:
            logger.exception(
                'Some runtime error has occurred in main IDL reduction routine - check IDL console logs for ObsID: ',
                obs_id)


        ####################################### Call FASPER LS routine and LS plotting routine ######################
        # call to IDL FASPER routine to produce Lomb-Scargle periodogram. Code held in separate imported script """
        try:
            fasper_data = base_dir + '/' + obs_id
            print fasper_data
            new_call_to_lomb_scargle_FASPER.fasper(fasper_data, ls_results_dir)
            new_call_to_lomb_scargle_FASPER.plot_results(ls_results_dir)
        except RuntimeError:
            logger.exception(
                'Some runtime error has occurred in main IDL reduction routine - check IDL console logs for ObsID: ',
                obs_id)

        ############################################ Call epoch folding routine #####################################
        # call to IDL epoch folding routine. Code held in separate imported script """
        try:
            epoch_data = base_dir + "/" + obs_id
            print epoch_data
            call_to_epoch_search.epoch_search(epoch_data, epoch_results_dir)
        except RuntimeError:
            logger.exception(
                'Some runtime error has occurred in main IDL reduction routine - check IDL console logs for ObsID: ',
                obs_id)




    #======================================   Remove unneeded directories from main results ========================
    # Remove unneeded directories/files from main results to keep size of tarred products to minimum.
    try:
        path_to_main_dir = base_dir + "/" + obs_id
        list_delete_files = os.listdir(path_to_main_dir)
        #remove unwanted directories
        for dir_name in list_delete_files:
            if dir_name in ('pps', 'odf', 'lib', 'aioclient'):
                remove_dir = "cd " + base_dir + "/" + obs_id + " && rm -r " + str(dir_name)
                print remove_dir
                call(remove_dir, shell=True, executable='/bin/bash')
            # remove unwanted .tar file
            front_filename, file_ext = os.path.splitext(dir_name)
            if file_ext == '.tar':
                remove_dir = "cd " + base_dir + "/" + obs_id + " && rm -r " + str(dir_name)
                call(remove_dir, shell=True, executable='/bin/bash')
    except RuntimeError:
        logger.exception('Some runtime error has occurred in removing directory ', obs_id)
    except IOError:
        logger.exception('IO error has occurred in removing directory ', obs_id)
        #~~ remove unneeded files from each results directory ~~~~~~~~~~~~~~~~~~~~~~~
        remove_files_location = path_to_main_dir + "/" + "processed" + "/" + "test" + "/"
        print "remove_files_location :" + str(remove_files_location)
        remove_files = os.listdir(remove_files_location)
        for file_name in remove_files:
            full_file_name = os.path.join(remove_files_location, file_name)
            if os.path.isfile(full_file_name):
                front_string = str(file_name)
                first_four_chars = front_string[0:4]
                if first_four_chars == 'xxxx':
                    remove_file = "cd " + remove_files_location + " && rm " + file_name
                    print remove_file
                    call(remove_file, shell=True, executable='/bin/bash')
                elif first_four_chars == 'FLTL':
                    remove_file = "cd " + remove_files_location + " && rm " + file_name
                    print remove_file
                    call(remove_file, shell=True, executable='/bin/bash')
                elif first_four_chars == 'lcts':
                    remove_file = "cd " + remove_files_location + " && rm " + file_name
                    print remove_file
                    call(remove_file, shell=True, executable='/bin/bash')

    ################### Bundle together and compress with lrzip ##############################################################################
    #                     use like lrztar -o compressed_path -L level_no target_dir

    try:
        #create tar file to hold downloaded data .tar file and processed results
        lrztar_up = "lrztar -o " + lrztar_output_dir + "/" + obs_id + "_lrzip" + " -L 7 -N -19 " + base_dir + "/" + obs_id + "/" + "processed" + "/" + "test"
        print lrztar_up
        call(lrztar_up, shell=True, executable='/bin/bash')
        print "reached lrzip"
    except RuntimeError:
        logger.exception(
            'Some runtime error has occurred in download data def  - with lrztarring files for ObsID: ', obs_id)
    except IOError:
        logger.exception('IO error has occurred in download data def  - with lrztarring files for ObsID: ', obs_id)



    #Repopulate list of all obsIDs to process and rerun this def
    ids = fetch_all_obsIDs()
    built_up_list = convert_and_insert_obsIDs(ids)
    # check list not empty then fetch next
    if built_up_list:
        download_data(built_up_list)


if __name__ == '__main__':
    #fetch all obsIDs from table    
    list_of_all_obsIDs = fetch_all_obsIDs()
    # call main body of program - download files, call IDL and produce lightcurves for each one
    converted_list_of_all_obsIDs = convert_and_insert_obsIDs(list_of_all_obsIDs)
    download_data(converted_list_of_all_obsIDs)
    #end_script
    sys.exit()


