import psycopg2
import sys
from subprocess import call
import cPickle

##########  Script with example code for importing products into postgres db.



#tar up processed products to insert into database, moves to processed dir, creates .tar.gz file and passes
#that to readImage to turn create binary object.  
def tarup_products(lcurve_obsID, light_curve_dir):
    try:
        tar_dir = "cd " + light_curve_dir + "/" + lcurve_obsID + " && " + "tar -czf " + lcurve_obsID + ".tar.gz " + light_curve_dir + "/" + lcurve_obsID + "/*.*" 
        print tar_dir 
        call(tar_dir, shell=True, executable='/bin/bash') 
        tar_file_name = light_curve_dir + "/" + lcurve_obsID + ".tar.gz"
        print tar_file_name
        return tar_file_name
    except IOError, e: 
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)

#read in tarred test dir file as binary object and write to table tarred_products 


def insert_background_lightcurve(bkg_lc_obsID, bkg_lc_obsNo, bkg_lc_energy, bkg_lc_time, bkg_lc_fits_file):
    try: 
        data_to_write = cPickle.dumps(bkg_lc_fits_file, protocol=1)
        conn = psycopg2.connect("dbname=raw_products user=postgres host=127.0.0.1 password=tuorla41")
        #open cursor
        cur = conn.cursor()
        sql = "INSERT INTO background_lc(bkg_lc_obsID, bkg_lc_obsNo, bkg_lc_energy, bkg_lc_time_res, bkg_lc_file) VALUES (%s, %s, %s, %s, %s)"
        cur.execute(sql, (bkg_lc_obsID, bkg_lc_obsNo, bkg_lc_energy, bkg_lc_time, (psycopg2.Binary(data_to_write),)))
        conn.commit()
        conn.close()
    except IOError, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)

def insert_background_spectrum(bkg_sp_obsID, bkg_sp_obsNo, bkg_sp_energy, bkg_sp_time_res, bkg_sp_fits_file):
    try:
        data_to_write = cPickle.dumps(bkg_sp_fits_file, protocol=1)
        conn = psycopg2.connect("dbname=raw_products user=postgres host=127.0.0.1 password=tuorla41")
        #open cursor
        cur = conn.cursor()
        sql = "INSERT INTO background_spectrum(bkg_sp_obsID, bkg_sp_obsNo, bkg_sp_energy, bkg_sp_time_res, bkg_sp_file) VALUES (%s, %s, %s, %s, %s)"
        cur.execute(sql, (bkg_sp_obsID, bkg_sp_obsNo, bkg_sp_energy, bkg_sp_time_res, (psycopg2.Binary(data_to_write),)))
        conn.commit()
        conn.close()
    except IOError, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
        
def insert_gtis(gti_obsID, gti_obsNo, gti_fits_file):
    try:
        data_to_write = cPickle.dumps(gti_fits_file, protocol=1)
        conn = psycopg2.connect("dbname=raw_products user=postgres host=127.0.0.1 password=tuorla41")
        #open cursor
        cur = conn.cursor()
        sql = "INSERT INTO gti(gti_obsID, gti_obsNo, gti_file) VALUES (%s, %s, %s)"
        cur.execute(sql, (gti_obsID, gti_obsNo, (psycopg2.Binary(data_to_write),)))
        conn.commit()
        conn.close()
    except IOError, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)

def insert_images(im_obsID, im_energy, im_fits_file):
    try:
        data_to_write = cPickle.dumps(im_fits_file, protocol=1)
        conn = psycopg2.connect("dbname=raw_products user=postgres host=127.0.0.1 password=tuorla41")
        #open cursor
        cur = conn.cursor()
        sql = "INSERT INTO images(image_obsID, image_energy, image_file) VALUES (%s, %s, %s)"
        cur.execute(sql, (im_obsID, im_energy, (psycopg2.Binary(data_to_write),)))
        conn.commit()
        conn.close()
    except IOError, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
         
     
def insert_FLTI(flti_obsID, flti_energy, flti_fits_file):
    try:
        data_to_write = cPickle.dumps(flti_fits_file, protocol=1)
        conn = psycopg2.connect("dbname=raw_products user=postgres host=127.0.0.1 password=tuorla41")
        #open cursor
        cur = conn.cursor()
        sql = "INSERT INTO flti(flti_obsID, flti_energy, flti_file) VALUES (%s, %s, %s)"
        cur.exectute(sql, (flti_obsID, flti_energy, (psycopg2.Binary(data_to_write),)))
        conn.commit()
        conn.close()
    except IOError, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
         
    
def insert_source_lightcurves(src_lc_obsID, src_lc_obsNo, src_lc_energy, src_lc_time_res, src_lc_fits_file):
    try:
        data_to_write = cPickle.dumps(src_lc_fits_file, protocol=1)
        conn = psycopg2.connect("dbname=raw_products user=postgres host=127.0.0.1 password=tuorla41")
        #open cursor
        cur = conn.cursor()
        sql = "INSERT INTO source_lc(src_lc_obsID, src_lc_obsNo, src_lc_energy, src_lc_time_res, src_lc_file) VALUES (%s, %s, %s, %s, %s)"
        cur.execute(sql, (src_lc_obsID, src_lc_obsNo, src_lc_energy, src_lc_time_res, (psycopg2.Binary(data_to_write),)))
        conn.commit()
        conn.close()
    except IOError, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1)
    
def insert_source_spectrum(src_sp_obsID, src_sp_obsNo, src_sp_energy, src_sp_time_res, src_sp_fits_file):
    try:
        data_to_write = cPickle.dumps(src_sp_fits_file, protocol=1)
        conn = psycopg2.connect("dbname=raw_products user=postgres host=127.0.0.1 password=tuorla41")
        #open cursor
        cur = conn.cursor()
        sql = "INSERT INTO source_spectrum(src_sp_obsID, src_sp_obsNo, src_sp_energy, src_sp_time_res, src_sp_file) VALUES (%s, %s, %s, %s, %s)"
        cur.execute(sql, (src_sp_obsID, src_sp_obsNo, src_sp_energy, src_sp_time_res, (psycopg2.Binary(data_to_write),)))
        conn.commit()
        conn.close()
    except IOError, e:
        print "Error %d: %s" % (e.args[0],e.args[1])
        sys.exit(1) 



#setup db connection and create table to store results of reduction process
#try:
#    conn = psycopg2.connect("dbname=raw_products user=postgres host=127.0.0.1 password=tuorla41")
    #open cursor
#    cur = conn.cursor()
#    cur.execute("DROP TABLE IF EXISTS lightcurves")
    #create new table to store the 6 processed lightcurves
#    cur.execute("CREATE TABLE lightcurves (lc_obsID INT PRIMARY KEY, tarred_products BYTEA)")
    #commit execute statement
#    conn.commit()
#    conn.close()
#except psycopg2.DatabaseError, e:
#    if conn:
#        conn.rollback()
#    print 'Error %s' % e
#    sys.exit(1)
#finally:
#    if conn:
#        conn.close()


            



#extracting energy ranges for input into db   !!!!!!! look into again.....
#    light_curve_dir = '/home/andy/disk/501/processed/test/'
#    f for f in os.listdir(light_curve_dir)
#       filename = os.path.split(f)
#        extension = os.path.spliext(filename)
#        #eng_range is list of energy names
#        eng_range.append(shortname)
        #lightcurve_filename is what it says
#        lightcurve_filename.append(f)


# Write fits files in binary format into table created above
#try:
#    conn = psycopg2.connect("dbname=raw_products user=postgres host=127.0.0.1 password=tuorla41")
    #open cursor
#    cur = conn.cursor()
    #loop to transform 6 lightcurve files from fits into binary objects via readImage function above.
#    counter = 0
#    while counter < 6:
#        data = readImage(f[counter])
#        binary = psycopg2.Binary(data)
#        cur.execute("INSERT into lightcurves(lc_obsID, energy_range, lightcurve_file) VALUES(lcurve_obsID, erange, binary)")
#        counter = counter + 1
#    cur.commit()
#    conn.close()
#except psycopg2.DatabaseError, e:
#    if con:
#        con.rollback()
#    print 'Error %s' % e
#    sys.exit(1)
#finally:
#    if con:
#        con.close()


    
#tarup_products(light_curve_dir)







