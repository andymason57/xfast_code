from subprocess import call
import os
import sys
import shutil
import logging

import psycopg2


# import script to run FASPER LS routine

# Main script to automate xfast data download, extraction and processing with SAS.

# TODO sort out selecting flare cut off from pipeline processed products leads to no GTIs
# TODO put in code to deal with clean/unclean obs/sources - IDL.

#global variable - path to download directory
from timing_analysis_codes import new_call_to_lomb_scargle_FASPER

base_dir = "/mnt/4tbdata/five"
lrztar_output_dir = "/mnt/4tbdata/five_results"


################################ Set up error logging facility ####################################################################### 

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


#############################  Retrieve all obsIDs from Postgres to process #########################################################################
def fetch_all_obsIDs():
    try:
        conn = psycopg2.connect("dbname=postgres user=postgres host=127.0.0.1 password=YvonneCSutton42")
        #open cursor
        fetch_data = conn.cursor()
        #select unique obs_ids from table all_unique
        fetch_data.execute("SELECT obs_id FROM test_cases_five;")
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


########################## Convert ObsIDs and insert into new table  #################################################################
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
                #append stripped obsID as integer into new list 'built_up_list' and write stripped obsID into db
                built_up_list.append(striped_row)
                #cur.execute("INSERT INTO  converted_obsID (conv_obs_id) VALUES (%s)", (striped_row,))
                #conn.commit()
            elif len(striped_row) == 9:
                striped_row = "0" + striped_row
                built_up_list.append(striped_row)
                #cur.execute("INSERT INTO  converted_obsID (conv_obs_id) VALUES (%s)", (striped_row,))
                #conn.commit()
            elif len(striped_row) == 8:
                striped_row = "00" + striped_row
                built_up_list.append(striped_row)
                #cur.execute("INSERT INTO  converted_obsID (conv_obs_id) VALUES (%s)", (striped_row,))
                #conn.commit()
            elif len(striped_row) == 7:
                striped_row = "000" + striped_row
                built_up_list.append(striped_row)
                #cur.execute("INSERT INTO  converted_obsID (conv_obs_id) VALUES (%s)", (striped_row,))
                #conn.commit()
        except RuntimeError:
            logger.exception('Error in def. convert_and_insert_obsIDs in appending zeros to string ID ' + str_row)
    return built_up_list


################################## Main part of script - Download data, reduce, compress and store results ##############################################################
# First fetch list of obsIDs to reduce from Postgres.  
# Then -- need to have a copy of aioclient and lib dir. in download directory
# send request to XSA to get str_row files of type odf and put them in str_row directory.
# Unpack download obs and move into new directory structure, then delete downloaded directories. 
# Then pass obsID to reduce to IDL scripts to begin reducing.
#Finally run lrzip to compress products of reduction by IDL and then delete the uncompressed results directories. 

def download_data(built_up_list):
    for str_row in built_up_list:
        try:
            # pull obs_id of stack
            conn = psycopg2.connect("dbname=postgres user=postgres host=127.0.0.1 password=YvonneCSutton42")
            #open cursor
            cur = conn.cursor()
            cur.execute("INSERT INTO test_cases_five_results (obs_id) VALUES (%s)", (str_row,))
            cur.execute("DELETE FROM test_cases_five WHERE obs_id = %s", (str_row,))
            conn.commit()
            conn.close()

        except psycopg2.DatabaseError:
            print "Database exception error... rolling back"
            logger.exception('Database exception error ... rolling back')
            if conn:
                conn.rollback()
                print 'Database Error %s'

        try:
            create_obsID_dir = "cd " + base_dir + " && mkdir " + str_row
            print create_obsID_dir
            call(create_obsID_dir, shell=True, executable='/bin/bash')
            #copy aioclient scripts to download directory
            copy_aioclient = "cd " + base_dir + " && cp -ar " + base_dir + "/lib " + base_dir + "/" + str_row + " && " + \
                             "cp -ar " + base_dir + "/aioclient " + base_dir + "/" + str_row + " && chmod u+x aioclient"
            print copy_aioclient

            call(copy_aioclient, shell=True, executable='/bin/bash')
            #call aioclient script to download data in str_row directory
            dir_str = "cd " + base_dir + "/" + str_row + " && ./aioclient -L " + '"GET obsno=' + str_row + " -O " + str_row + '"'

            #call to shell to execute aioclient with neccessary parameters
            call(dir_str, shell=True, executable='/bin/bash')

            ###################################### Untar download files #############################################################################
            # uses python routine from http://guanidene.blogspot.fi/2011/06/nested-tar-archives-extractor.html
            # Make sure extractnested.py is included in folder within PATH. Also set permissions chmod ugo+rx extractnested.py
            # the in Call string use path to extractnested.py script and also need executable='/bin/bash' otherwise /bin/sh shell is called by default
            # LOCAL PATH TO BE CHANGED!!!!

            #have to rename downloaded .tar file from GUEST**** to str_row.tar
            rename_downloaded_tarfile = "cd " + base_dir + "/" + str_row + " && " + "for i in ./*GUEST*;do mv -- " + '"$i"' + " " + '"' + str_row + '.tar' + '"' + ";done"
            call(rename_downloaded_tarfile, shell=True, executable='/bin/bash')
            unpack_str = base_dir + '/extractnested.py' + " " + base_dir + "/" + str_row + "/" + str_row + ".tar"
            call(unpack_str, shell=True, executable='/bin/bash')

            ################################### Move files into required directories #################################################################

            #set up new directory structure in download directory
            #!!!!! Don't think I need this moving/copying directories - just going to store the downloaded tar file. So just need to unpack the contents of untarred dir.
            move_to_download_dir = "cd " + base_dir + "/" + str_row + " && mkdir odf && mkdir pps && mkdir processed"
            print move_to_download_dir
            call(move_to_download_dir, shell=True, executable='/bin/bash')
            set_up_subdir = "cd " + base_dir + "/" + str_row + "/processed && mkdir test"
            call(set_up_subdir, shell=True, executable='/bin/bash')
            #Copy odf and pps files into new directories.
            src = base_dir + "/" + str_row + "/" + str_row + "/" + str_row + "/odf" + "/" + str_row + "/"
            odf_dest = base_dir + "/" + str_row + "/odf/"
            #Copy files from downloaded odf dir. to new odf dir. If file is actually directory obtain the name of it.
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
            #initialise path of main downloaded pps dir and path of new pps dir.
            src_pps = base_dir + "/" + str_row + "/" + str_row + "/" + str_row + "/pps"
            pps_dest = base_dir + "/" + str_row + "/pps/"
            #Copy files from downloaded pps dir. to new pps dir.
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
            #remove untarred download directory
            remove_untarred = "rm -R " + base_dir + "/" + str_row + "/" + str_row
            print remove_untarred
            call(remove_untarred, shell=True, executable='/bin/bash')
        except IOError:
            logger.exception('Error: can\'t find file or read data')
            logger.exception('Error occured in moving files in DOWNLOAD_DATA def... ' + ' the obsID is ', str_row)


        ######################################### Call Main IDL reduction scripts ###################################################################

        #Call Lucy's IDL script to reduce data - need to pass directory!

        try:
            idl_call = "cd " + base_dir + " && idl -e xmmpipeworkingv29_five -args " + " " + base_dir + "/" + str_row + " " + str_row
            call(idl_call, shell=True, executable='/bin/bash')
        except RuntimeError:
            logger.exception(
                'Some runtime error has occurred in main IDL reduction routine - check IDL console logs for ObsID: ',
                str_row)



        #======================================   Remove unneeded directories from main results =========================================================================
        #             try:
        #                 path_to_main_dir = base_dir + "/" + str_row
        #                 list_delete_files = os.listdir(path_to_main_dir)
        #                 #remove unwanted directories
        #                 for dir_name in list_delete_files:
        #                     if dir_name in ('pps', 'odf','lib','aioclient'):
        #                         remove_dir = "cd " + base_dir + "/" + str_row + " && rm -r " + str(dir_name)
        #                         print remove_dir
        #                         call(remove_dir, shell = True, executable='/bin/bash')
        #                     # remove unwanted .tar file
        #                     front_filename, file_ext = os.path.splitext(dir_name)
        #                     if file_ext == '.tar':
        #                         remove_dir = "cd " + base_dir + "/" + str_row + " && rm -r " + str(dir_name)
        #                         call(remove_dir, shell = True, executable='/bin/bash')
        #             except RuntimeError:
        #                     logger.exception('Some runtime error has occurred in removing directory ', str_row)
        #             except IOError:
        #                     logger.exception('IO error has occurred in removing directory ', str_row)
        # #
        #
        # #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ remove unneeded files from each results directory ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        #             remove_files_location = path_to_main_dir + "/" + "processed" + "/" + "test" + "/"
        #             print "remove_files_location :" + str(remove_files_location)
        #             remove_files = os.listdir(remove_files_location)
        #             for file_name in remove_files:
        #                 full_file_name = os.path.join(remove_files_location, file_name)
        #                 if (os.path.isfile(full_file_name)):
        #                     front_string = str(file_name)
        #                     first_four_chars = front_string[0:4]
        #                     if first_four_chars == 'xxxx':
        #                         remove_file = "cd " + remove_files_location + " && rm " + file_name
        #                         print remove_file
        #                         call(remove_file, shell = True, executable='/bin/bash')
        #                     elif first_four_chars == 'FLTL':
        #                         remove_file = "cd " + remove_files_location + " && rm " + file_name
        #                         print remove_file
        #                         call(remove_file, shell = True, executable='/bin/bash')
        #                     elif first_four_chars == 'lcts':
        #                         remove_file = "cd " + remove_files_location + " && rm " + file_name
        #                         print remove_file
        #                         call(remove_file, shell = True, executable='/bin/bash')
        #
        #################### Bundle together and compress with lrzip ##############################################################################
        #                      use like lrztar -o compressed_path -L level_no target_dir
        try:
            #create tar file to hold downloaded data .tar file and processed results
            lrztar_up = "lrztar -o " + lrztar_output_dir + "/" + str_row + "_lrzip" + " -L 7 -N -19 " + base_dir + "/" + str_row + "/" + "processed" + "/" + "test"
            print lrztar_up
            call(lrztar_up, shell=True, executable='/bin/bash')
            print "reached lrzip"
        except RuntimeError:
            logger.exception(
                'Some runtime error has occurred in download data def  - with lrztarring files for ObsID: ', str_row)
        except IOError:
            logger.exception('IO error has occurred in download data def  - with lrztarring files for ObsID: ', str_row)


            #Repopulate list of all obsIDs to process and rerun this def
        ids = fetch_all_obsIDs()
        built_up_list = convert_and_insert_obsIDs(ids)
        download_data(built_up_list)
        #close connection to db
        #conn.close()


if __name__ == '__main__':
    #fetch all obsIDs from table    
    list_of_all_obsIDs = fetch_all_obsIDs()
    # call main body of program - download files, call IDL and produce lightcurves for each one
    converted_list_of_all_obsIDs = convert_and_insert_obsIDs(list_of_all_obsIDs)
    download_data(converted_list_of_all_obsIDs)


    #end_script
    sys.exit()


