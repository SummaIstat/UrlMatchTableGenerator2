import logging
import os
import sys
import pandas as pd
from datetime import datetime
import tldextract

# logging configuration
logger = logging.getLogger('UrlMatchTableGenerator2Logger')
logger.setLevel(logging.DEBUG)
logFormatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S")
consoleHandler = logging.StreamHandler(sys.stdout)
consoleHandler.setLevel(logging.DEBUG)
consoleHandler.setFormatter(logFormatter)
logger.addHandler(consoleHandler)
now = datetime.now()
dateTime = now.strftime("%Y-%m-%d_%H_%M_%S")
LOG_FILE_NAME = "UrlMatchTableGenerator2_" + dateTime + ".log"
fileHandler = logging.FileHandler(LOG_FILE_NAME)
fileHandler.setLevel(logging.DEBUG)
fileHandler.setFormatter(logFormatter)
logger.addHandler(fileHandler)

FIRMS_FILE = ""
LINKS_SCORES_FILE = ""
OUTPUT_FILE_FOLDER = ""
LOG_FILE_FOLDER = ""
LOG_LEVEL = ""


def main(argv):
    logger.info("***************************************************")
    logger.info("**********   UrlMatchTableGenerator2   *************")
    logger.info("***************************************************\n\n")

    now = datetime.now()
    dateTime = now.strftime("%Y-%m-%d %H:%M:%S")
    logger.info("Starting datetime: " + dateTime)

    load_external_configuration()
    firm_list = load_firm_list(FIRMS_FILE)[1:]  # ignora l'header del file
    links_df = load_links_scores_data_frame(LINKS_SCORES_FILE)
    output_file = get_output_file()

    with open(output_file, 'a+', encoding='utf-8') as f:

        f.writelines(
            "FIRM_ID" + "\t" +
            "FIRM_NAME" + "\t" +
            "LINK_POS" + "\t" +
            "SIMPLE_URL" + "\t" +
            "NAME_IN_URL" + "\t" +
            "DOMAIN_IN_PEC1" + "\t" +
            "DOMAIN_IN_PEC2" + "\t" +
            "TEL" + "\t" +
            "VAT" + "\t" +
            "MUN" + "\t" +
            "PROV" + "\t" +
            "ZIP" + "\t" +
            "SCORE" + "\t" +
            "URL_WE_HAD" + "\t" +
            "URL_FOUND" + "\t" +
            "DOMAIN_HAD" + "\t" +
            "DOMAIN_FOUND" + "\t" +
            "URL_MATCH" + "\t" +
            "DOMAIN_MATCH" + "\t" +
            "DOMAIN_NO_EXT_MATCH" + "\n")
        f.flush()
        for i, firm in enumerate(firm_list):
            logger.info(str(i+1) + " / " + str(len(firm_list)) + " I am processing the firm having id = " + firm[0])
            firm_id = firm[0]
            firm_name = firm[1]
            url_we_had = firm[2]
            pec = firm[3]
            links_by_firm_id_df = links_df[links_df["FIRM_ID"] == firm_id]
            links_by_firm_id_df = links_by_firm_id_df.sort_values(by=['LINK_POSITION'])
            for index, row in links_by_firm_id_df.iterrows():
                ext_had = tldextract.extract(url_we_had)
                domain_had = ext_had.domain + "." + ext_had.suffix

                ext_found = tldextract.extract(row['URL'])
                domain_found = ext_found.domain + "." + ext_found.suffix

                if url_we_had == row['URL']:
                    url_match = "1"
                else:
                    url_match = "0"

                if domain_had == domain_found:
                    domain_match = "1"
                else:
                    domain_match = "0"

                if ext_had.domain == ext_found.domain:
                    domain_no_ext_match = "1"
                else:
                    domain_no_ext_match = "0"

                name_in_url = "0"
                if is_subname_in_url(firm_name, row['URL']):
                    name_in_url = "1"

                domain_in_pec1 = get_domain_in_pec1(ext_found.domain, pec)
                domain_in_pec2 = get_domain_in_pec2(domain_found, pec)
                score_vector = row['SCORE_VECTOR']
                tel = get_tel(score_vector)
                simple_url = get_simple_url(row['URL'])
                link_pos = str(row['LINK_POSITION'])
                vat = score_vector[3:4]
                mun = score_vector[4:5]
                prov = score_vector[5:6]
                zip_code = score_vector[6:7]

                f.writelines(
                    row['FIRM_ID'] + "\t" +
                    firm_name + "\t" +
                    link_pos + "\t" +
                    simple_url + "\t" +
                    name_in_url + "\t" +
                    domain_in_pec1 + "\t" +
                    domain_in_pec2 + "\t" +
                    tel + "\t" +
                    vat + "\t" +
                    mun + "\t" +
                    prov + "\t" +
                    zip_code + "\t" +
                    row['SCORE'] + "\t" +
                    url_we_had + "\t" +
                    row['URL'] + "\t" +
                    domain_had + "\t" +
                    domain_found + "\t" +
                    url_match + "\t" +
                    domain_match + "\t" +
                    domain_no_ext_match + "\n")
                f.flush()

    now = datetime.now()
    dateTime = now.strftime("%Y-%m-%d %H:%M:%S")
    logger.info("Ending datetime: " + dateTime)


def get_tel(score_vector):
    tel = score_vector[:1]
    if tel == "1":
        tel = "0"
    else:
        tel = "1"
    return tel


def get_simple_url(url):
    result = "0"
    extracted = tldextract.extract(url)
    domain_name = extracted.domain + "." + extracted.suffix
    tokens = url.split(domain_name)
    if len(tokens) == 2:
        if len(tokens[1]) <= 1:
            result = "1"
    return result


def get_domain_in_pec1(domain_no_ext, pec):
    result = "0"
    if len(pec) != 0:
        tokens = pec.split("@")
        if len(tokens) == 2:
            if domain_no_ext in tokens[0]:
                result = "1"
    return result


def get_domain_in_pec2(domain_with_ext, pec):
    result = "0"
    if len(pec) != 0:
        tokens = pec.split("@")
        if len(tokens) == 2:
            if domain_with_ext in tokens[1]:
                result = "1"
    return result


def is_subname_in_url(firm_name, url):
    # Se almeno un token nel nome azienda è contenuto nel dominio dell'url ritorna True
    result = False
    firm_name = firm_name.replace(".", "")
    firm_name = firm_name.replace(",", " ")
    firm_name = firm_name.replace("'", " ")
    firm_name = firm_name.replace("-", " ")
    firm_name = firm_name.replace('"', ' ')
    tokens = firm_name.split(" ")
    extracted = tldextract.extract(url)
    domain_name = extracted.domain + "." + extracted.suffix
    for token in tokens:
        if (len(token) > 2) and (token.lower() in domain_name.lower()):
            result = True
            break
    return result


def load_firm_list(firms_file):
    firm_list = []

    with open(firms_file, "rt") as f:
        for line in f.readlines():
            tokens = line.split("\t")
            if len(tokens) == 4:
                my_tuple = (tokens[0].rstrip(), tokens[1].rstrip(), tokens[2].rstrip(), tokens[3].rstrip())
                firm_list.append(my_tuple)
            else:
                logger.warning("the firm having id=" + tokens[0] + " is malformed and will not be considered !")

    return firm_list


def load_links_scores_data_frame(links_scores_file):
    mydtypes = {
        "FIRM_ID": "str",
        "LINK_POSITION": "int",
        "URL": "str",
        "SCORE_VECTOR": "str",
        "SCORE": "str"
    }
    links_df = pd.read_csv(links_scores_file, sep="\t", dtype=mydtypes, encoding="utf8")
    return links_df


def get_output_file():
    now = datetime.now()
    dateTime = now.strftime("%Y-%m-%d_%H_%M_%S")
    output_file_name = "match_table_" + dateTime + ".csv"
    output_file = output_file_name
    if os.path.isdir(OUTPUT_FILE_FOLDER):
        output_file = OUTPUT_FILE_FOLDER + "/" + output_file_name
    return output_file


def load_external_configuration():
    global FIRMS_FILE
    global LINKS_SCORES_FILE
    global OUTPUT_FILE_FOLDER
    global LOG_FILE_FOLDER
    global LOG_FILE_NAME
    global LOG_LEVEL
    global consoleHandler
    global fileHandler
    global logger

    config_file = "config.cfg"
    if not os.path.isfile(config_file):
        logger.error("No \"config.cfg\" configuration file found in the program directory !")
        raise FileNotFoundError("No \"config.cfg\" configuration file found in the program directory !")

    external_settings = dict()
    with open(config_file, "rt") as f:
        for line in f.readlines():
            if not line.startswith("#"):
                tokens = line.split("=")
                if len(tokens) == 2:
                    external_settings[tokens[0]] = tokens[1]

    FIRMS_FILE = str(external_settings.get("FIRMS_FILE", "")).rstrip()
    if not os.path.isfile(FIRMS_FILE):
        logger.error("Invalid FIRMS_FILE parameter !")
        raise FileNotFoundError("Invalid FIRMS_FILE parameter !")

    LINKS_SCORES_FILE = str(external_settings.get("LINKS_SCORES_FILE", "")).rstrip()
    if not os.path.isfile(LINKS_SCORES_FILE):
        logger.error("Invalid LINKS_SCORES_FILE parameter !")
        raise FileNotFoundError("Invalid LINKS_SCORES_FILE parameter !")

    OUTPUT_FILE_FOLDER = str(external_settings.get("OUTPUT_FILE_FOLDER", "")).rstrip()
    if not os.path.isdir(OUTPUT_FILE_FOLDER):
        logger.warning("Invalid OUTPUT_FILE_FOLDER parameter, the default location will be used !")

    LOG_FILE_FOLDER = str(external_settings.get("LOG_FILE_FOLDER", "")).rstrip()
    now = datetime.now()
    dateTime = now.strftime("%Y-%m-%d_%H_%M_%S")
    if os.path.isdir(LOG_FILE_FOLDER):
        fileHandler = logging.FileHandler(LOG_FILE_FOLDER + "/" + LOG_FILE_NAME)
    else:
        logger.warning("Invalid LOG_FILE_FOLDER parameter, the default logfile location will be used !")

    LOG_LEVEL = str(external_settings.get("LOG_LEVEL", "INFO")).rstrip()
    consoleHandler.setLevel(LOG_LEVEL)
    fileHandler.setLevel(LOG_LEVEL)


if __name__ == "__main__":
    main(sys.argv[1:])
