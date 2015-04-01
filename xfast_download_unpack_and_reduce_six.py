########################################################################################################################
##                                                                                                                    ##
##             Main Python script to automate xfast data download, extraction and processing with SAS.                ##
##             Author : Andrew Mason - 26 March, 2015.                                                                ##
##                                                                                                                    ##
##              Calls IDL routine - idl_xmm_pipe_XXX - need to hardcode the directory where the data is kept in this  ##
##              script before this routine is run                                                                     ##
##                                                                                                                    ##
########################################################################################################################

__author__ = 'andmas'
from subprocess import call
import os
import sys
import shutil
import logging
# import python postgres database driver
import psycopg2
#import filematching module
import fnmatch
# import code module to perform lomb_scargle analysis
from timing_analysis_codes import new_call_to_lomb_scargle_FASPER


############################### Global variables #######################################################################
# !!!!!!!!!!!!   Need to set these for each individual process/terminal instance !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# Paths to obs to process and results directory
base_dir = "/mnt/4tbdata/six"
output_dir = "/mnt/4tbdata/six_processed"
# Names of tables used to store obs_ids to process and list of processed obs_ids
to_process_table = "to_process_six"
processed_table = "processed_six"
# Name of IDL routine to call to perform reduction. Must call separate IDL script for each reduction instance.
idl_routine = "idl_xmm_pipe_six"


################################ Set up error logging facility #########################################################
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
logger.setLevel(logging.DEBUG)


#############################  Retrieve all obsIDs from Postgres to process ############################################
def fetch_all_obsIDs():
    try:
        #set up connection to db
        conn = psycopg2.connect("dbname=postgres user=postgres host=127.0.0.1 password=YvonneCSutton42")
        #open cursor
        fetch_data = conn.cursor()
        #select unique obs_ids from table holding obs_ids
        to_process_sql_string = "SELECT obs_id FROM " + to_process_table + ";"
        fetch_data.execute(to_process_sql_string)
        #commit execute statement
        conn.commit()

        #Fetch all obs_ids from all_unique Table
        ids = fetch_data.fetchall()
        #close cursor
        fetch_data.close()
        conn.close()

        return ids

    # exception handler to issue alert with db connection problems
    except psycopg2.DatabaseError:
        print "I am unable to connect to the database"
        logger.exception('I am unable to connect to the database')
        if conn:
            conn.rollback()
            print 'Error %s'
            sys.exit(1)


########################## Convert ObsIDs into standard 10 character string format #####################################
def convert_and_insert_obsIDs(convert_ids):
    #initialise empty list
    built_up_list = []

    for row in convert_ids:
        try:
            #need to convert list elements into strings
            str_row = str(row)
            #strip off leading/ending ( and ) and , characters from string
            striped_row = str_row.strip('\'(),')
            #Convert all obs_ids to be of length 10 characters.
            if len(striped_row) == 10:
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
    #return convert list of obs_ids
    return built_up_list


############################# Main part of script - Download data, reduce, compress and store results ##################
# First fetch list of obsIDs to reduce from Postgres.
# Update tables.
# Send request to XSA to get data based on ObsId.
# Unpack downloaded data and move into new directory structure, then delete downloaded directories.
# Then pass obsID data to IDL scripts to begin reducing.
# Finally run lrzip to compress reduction products and then delete the uncompressed results.


def download_data(built_up_list):
     # insert obs_id that is being processed into processed table and delete from to_process main table
    for obsId in built_up_list:
        try:
            conn = psycopg2.connect("dbname=postgres user=postgres host=127.0.0.1 password=YvonneCSutton42")
            cur = conn.cursor()
            processed_sql_string = "INSERT INTO " + processed_table + "(obs_id) VALUES (%s)"
            cur.execute(processed_sql_string, (obsId,))
            delete_sql_string = "DELETE FROM " + to_process_table + " WHERE obs_id = %s"
            cur.execute(delete_sql_string, (obsId,))
            conn.commit()
            conn.close()

        #exception handler if problem fetching obs_id, inserting into processed list rollback db.
        except psycopg2.DatabaseError:
            print "Database exception error... rolling back"
            logger.exception('Database exception error ... rolling back')
            if conn:
                conn.rollback()
                print 'Database Error %s'

        try:
            # create working dir for each obs_id
            create_obsID_dir = "cd " + base_dir + " && mkdir " + obsId
            call(create_obsID_dir, shell=True, executable='/bin/bash')

            #copy aioclient script  to each obs_id directory
            copy_aioclient = "cd " + base_dir + " && cp -ar " + base_dir + "/lib " + base_dir + "/" + obsId + \
                             " && " + "cp -ar " + base_dir + "/aioclient " + base_dir + "/" + obsId + \
                             " && chmod u+x aioclient"
            call(copy_aioclient, shell=True, executable='/bin/bash')

            #call aioclient script to download data in obs_id directory
            dir_str = "cd " + base_dir + "/" + obsId + " && ./aioclient -L " + '"GET obsno=' + obsId + " -O " + \
                      obsId + '"'
            call(dir_str, shell=True, executable='/bin/bash')

            # Untar downloaded files
            # uses python routine from http://guanidene.blogspot.fi/2011/06/nested-tar-archives-extractor.html
            # Must set permissions chmod ugo+rx extractnested.py and also need to set extractednested.py to use the
            # executable='/bin/bash' otherwise /bin/sh shell is called by default

            #have to rename downloaded .tar file from GUEST**** to obsId.tar
            rename_downloaded_tarfile = "cd " + base_dir + "/" + obsId + " && " + "for i in ./*GUEST*;do mv -- " + \
                                        '"$i"' + " " + '"' + obsId + '.tar' + '"' + ";done"
            call(rename_downloaded_tarfile, shell=True, executable='/bin/bash')

            #unpack tar file using extractnested.py
            unpack_str = base_dir + '/extractnested.py' + " " + base_dir + "/" + obsId + "/" + obsId + ".tar"
            call(unpack_str, shell=True, executable='/bin/bash')

            # Move files into required directories - set up new directory structure in download directory
            #first create required dirs. odf, pps and main results dir. processed
            move_to_download_dir = "cd " + base_dir + "/" + obsId + " && mkdir odf && mkdir pps && mkdir processed"
            call(move_to_download_dir, shell=True, executable='/bin/bash')

            # create subdir test in processed dir to store all results of processing
            set_up_subdir = "cd " + base_dir + "/" + obsId + "/processed && mkdir test"
            call(set_up_subdir, shell=True, executable='/bin/bash')

            #extract odf and pps files from downloaded tar file and move into new directories.
            src = base_dir + "/" + obsId + "/" + obsId + "/" + obsId + "/odf" + "/" + obsId + "/"
            odf_dest = base_dir + "/" + obsId + "/odf/"
            src_files = os.listdir(src)
            for file_name in src_files:
                full_file_name = os.path.join(src, file_name)
                if (os.path.isfile(full_file_name)):
                    shutil.copy(full_file_name, odf_dest)
                elif (os.path.isdir(full_file_name)):
                    sub_dir = os.path.join(src, file_name)

            #list files in downloaded odf sub dir. and copy to new odf dir.
            src_files = os.listdir(sub_dir)
            for file_name in src_files:
                full_file_name = os.path.join(sub_dir, file_name)
                if (os.path.isfile(full_file_name)):
                    shutil.copy(full_file_name, odf_dest)

            #Copy files from downloaded pps dir. to new pps dir.
            src_pps = base_dir + "/" + obsId + "/" + obsId + "/" + obsId + "/pps"
            pps_dest = base_dir + "/" + obsId + "/pps/"
            src_files = os.listdir(src_pps)
            for file_name in src_files:
                full_file_name = os.path.join(src_pps, file_name)
                if (os.path.isfile(full_file_name)):
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

            #copy event file into main test directory
            list_pps_files = os.listdir(pps_dest)
            for file in list_pps_files:
                if fnmatch.fnmatch(file,'*PIEVLI*.fit'):
                    event_filename = file
                    print "filename found " + event_filename
            copy_event = "cp " + pps_dest + event_filename + " " + base_dir + "/" + obsId + "/processed/test/"
            call(copy_event, shell=True, executable='/bin/bash')

            #remove untarred downloaded directory
            remove_untarred = "rm -R " + base_dir + "/" + obsId + "/" + obsId
            call(remove_untarred, shell=True, executable='/bin/bash')

        except IOError:
            logger.exception('Error: can\'t find file or read data')
            logger.exception('Error occured in moving files in DOWNLOAD_DATA def... ' + ' the obsID is ', obsId)


        ######################################### Call Main IDL reduction scripts ######################################
        #Call Lucy's IDL script to reduce data. Need to pass main dir. path,obs_id dir. path and obs_id into IDL routine
        try:
            idl_call = "cd " + base_dir + " && idl -e " + idl_routine + " -args " + " " + base_dir + "/" + obsId + \
                       " " + obsId
            call(idl_call, shell=True, executable='/bin/bash')
        except RuntimeError:
            logger.exception(
                'Some runtime error has occurred in main IDL reduction routine - check IDL console logs for ObsID: ',
                obsId)

        ##########################################  Remove unneeded directories from main results ######################
        try:
            path_to_main_dir = base_dir + "/" + obsId
            list_delete_files = os.listdir(path_to_main_dir)
            #remove unwanted directories
            for dir_name in list_delete_files:
                if dir_name in ('pps', 'odf','lib','aioclient'):
                    remove_dir = "cd " + base_dir + "/" + obsId + " && rm -r " + str(dir_name)
                    print remove_dir
                    call(remove_dir, shell = True, executable='/bin/bash')
                # remove unwanted .tar file
                front_filename, file_ext = os.path.splitext(dir_name)
                if file_ext == '.tar':
                    remove_dir = "cd " + base_dir + "/" + obsId + " && rm -r " + str(dir_name)
                    call(remove_dir, shell = True, executable='/bin/bash')

             ############################################   Remove unneeded files from each results directory ###############
            remove_files_location = path_to_main_dir + "/" + "processed" + "/" + "test" + "/"
            print "remove_files_location :" + str(remove_files_location)
            remove_files = os.listdir(remove_files_location)
            for file_name in remove_files:
                full_file_name = os.path.join(remove_files_location, file_name)
                if (os.path.isfile(full_file_name)):
                    front_string = str(file_name)
                    first_four_chars = front_string[0:4]
                    #remove spectra created to calculate BACKSCAL - as not needed
                    if first_four_chars == 'xxxx':
                        remove_file = "cd " + remove_files_location + " && rm " + file_name
                        print remove_file
                        call(remove_file, shell = True, executable='/bin/bash')
                    elif first_four_chars == 'FLTL':
                        remove_file = "cd " + remove_files_location + " && rm " + file_name
                        print remove_file
                        call(remove_file, shell = True, executable='/bin/bash')
                    elif first_four_chars == 'lcts':
                        remove_file = "cd " + remove_files_location + " && rm " + file_name
                        print remove_file
                        call(remove_file, shell = True, executable='/bin/bash')


        except RuntimeError:
            logger.exception('Some runtime error has occurred in removing directory ', obsId)
        except IOError:
            logger.exception('IO error has occurred in removing directory ', obsId)



        #############################################  Bundle together and compress with lrzip ########################
        # use like lrztar -o compressed_path -L level_no target_dir
        try:
            #create tar file to hold downloaded data .tar file and processed results
            lrztar_up = "lrztar -o " + output_dir + "/" + obsId + "_lrzip" + " -L 7 -N -19 " + base_dir + \
                        "/" + obsId + "/" + "processed" + "/" + "test"
            call(lrztar_up, shell=True, executable='/bin/bash')
        except RuntimeError:
            logger.exception(
                'Some runtime error has occurred in download data def  - with lrztarring files for ObsID: ', obsId)
        except IOError:
            logger.exception('IO error has occurred in download data def  - with lrztarring files for ObsID: ', obsId)


        #Populate new list of all obsIDs to process and rerun code
        ids = fetch_all_obsIDs()
        built_up_list = convert_and_insert_obsIDs(ids)
        download_data(built_up_list)



if __name__ == '__main__':
    #fetch all obsIDs from table
    list_of_all_obsIDs = fetch_all_obsIDs()
    # call main body of program - download files, call IDL and produce lightcurves for each one
    converted_list_of_all_obsIDs = convert_and_insert_obsIDs(list_of_all_obsIDs)
    download_data(converted_list_of_all_obsIDs)
    #end_script
    sys.exit()



