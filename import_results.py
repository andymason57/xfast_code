__author__ = 'andmas_local'

import psycopg2

def write_to_file(pulsar_results):
    out_file = "/home/UTU/andmas/Desktop/pasi_shortperiod_results/out_results4.txt"
    target = open(out_file, 'a')
    for item in pulsar_results:
        csv = str(item) + ","
        csv = csv.translate(None, '()\'')

    csv = csv + "\n"
    print csv
    target.write(csv)
    target.close()

def fetch_results_from_db(obsID, detID):
     #set up connection to db
    conn = psycopg2.connect("dbname=postgres user=postgres host=127.0.0.1 password=YvonneCSutton42")
    #open cursor
    fetch_data = conn.cursor()
    #select reults from db
    results_needed = "SELECT obs_id, detid, srcid, iauname, obs_id, revolut, mjd_start, ra, dec, poserr,lii,bii,radec_err,syserrcc from xmm3dr4 where obs_id='" \
                     + obsID + "' and detid='" + detID + "';"
    print results_needed
    fetch_data.execute(results_needed)
    conn.commit()

    pulsar_results = fetch_data.fetchall()
    fetch_data.close()
    conn.close()

    write_to_file(pulsar_results)


def read_from_file():
    fname = "/home/UTU/andmas/Desktop/pasi_shortperiod_results/psd_results4"
    with open(fname) as f:
        content = f.readlines()

    f.close()
    return content

def get_obsid_detid(results_list):

    for line in results_list:
        full_line = line.split("_")
        temp_obsid = full_line[0]
        obsid = temp_obsid[2:]
        detid = full_line[1]
        fetch_results_from_db(obsid,detid)



if __name__ == '__main__':
    results_needed = read_from_file()
    get_obsid_detid(results_needed)
