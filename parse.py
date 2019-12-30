import configparser
import re
from pandas import DataFrame


def parse_countries(text):
    """
        Parses countries information from SAS definitions
    Args:
        text: Text of SAS file containing definitions for I94 immigration records

    Returns:
        List of countries containing tuples of Id, Name of country
    """
    contries_text = re.search(r'I94CIT.+?;', text, re.DOTALL).group(0)
    matches = re.findall(r'(.+)=(.+)', contries_text)
    matches.sort(key=lambda m: int(m[0]))
    countries = []

    countries.append((0, 'Other'))
    for (cid, cname) in matches:

        if not (re.findall(r'No Country*|INVALID*|Collapsed.*', cname, re.IGNORECASE)):
            countries.append((int(cid.replace("'", "").strip()), cname.replace("'", "").strip()))
    return countries


def write_csv(filename, list, cols):
    """
        Creates CSV file for given data
    Args:
        filename:  Name of file to create
        list: list of tuples of data to be written
        cols: list of columns for tuple data supplied

    Returns:
        None.
    """
    print ( "Creating csv file {} ".format(filename) )
    df = DataFrame(list, columns=cols)
    df.to_csv(filename, header=True, index=None)


def parse_port(text):
    """
        Parses ports information from SAS definitions
    Args:
        text: Text of SAS file containing definitions for I94 immigration records

    Returns:
        List of ports containing tuples of Id, City, State

    """
    port_text = re.search(r'I94PORT.+?;', text, re.DOTALL).group(0)
    matches = re.findall(r'(.+)=(.+)', port_text)
    matches.sort(key=lambda m: m[0])
    ports = []

    ports.append( (0, 'N/A','N/A'))
    for (cid, cname) in matches:
        cid = cid.replace("'", "").strip()
        cname = cname.replace("'", "").strip().split(",")
        if (len(cname) == 2):
            ports.append((cid, cname[0].strip(), cname[1].strip()))

    return ports


def create_mode(filename):
    """
        Create Transport modes data
    Args:
        filename:  Name of file to be created for Transport Modes, this is statically defined

    Returns:
        None
    """
    modes = []
    modes.append(("1", "Air"))
    modes.append(("2", "Sea"))
    modes.append(("3", "Land"))
    modes.append(("4", "Not reported"))
    modes.append(("0", "Not Available"))
    write_csv( filename, modes, ['code', 'name'])


def create_visatypes(filename):
    """
        Creates visatypes data
    Args:
        filename:  Name of file to be created for Visa Types, this is statically defined

    Returns:
        None
    """

    visatypes = []
    visatypes.append(("CP", "Commerical Pilot"))
    visatypes.append(("CPL", "Commerical Pilot"))
    visatypes.append(("B1", "Visa Holders-Business"))
    visatypes.append(("B2", "Visa Holders-Pleasure"))
    visatypes.append(("E1", "Visa Holders-Treaty Trader"))
    visatypes.append(("E2", "Visa Holders-Treaty Investor"))
    visatypes.append(("F1", "Visa Holders-Students"))
    visatypes.append(("F2", "Visa Holders-Family Members of Students"))
    visatypes.append(("GMB", "Guam Visa Waiver-Business"))
    visatypes.append(("GMT", "Guam Visa Waiver-Tourist"))
    visatypes.append(("I", "Visa Holders-Foreign Information Media"))
    visatypes.append(("M1", "Visa Holders-Vocational Students"))
    visatypes.append(("M2", "Visa Holders-Family Members of Vocational Students"))
    visatypes.append(("SBP", "Unknown Type"))
    visatypes.append(("WB", "Visa Waiver-Business"))
    visatypes.append(("WT", "Visa Waiver-Tourist"))
    visatypes.append(("NA", "Not Available"))

    write_csv( filename, visatypes, ['code', 'name'])


def create_visacodes(filename):
    """
        Creates visa codes data
    Args:
        filename:  Name of file to be created for Visa Codes, this is statically defined

    Returns:
        None
    """
    visacodes = []
    visacodes.append(("1", "Business"))
    visacodes.append(("2", "Pleasure"))
    visacodes.append(("3", "Student"))
    visacodes.append(("0", "Not Available"))

    write_csv(filename, visacodes, ['code', 'name'])


def main():
    """
        Driver method to create static data
    Returns:

    """

    config = configparser.ConfigParser()
    config.read('dl.cfg')

    with open(config['DATA']['SAS_FILE_DESCRIPTION'], 'r') as f:
        text = f.read()

    countries = parse_countries(text)
    ports = parse_port(text)

    # output_dir is input for next phase.
    output_dir = config['DATA']['INPUT_DIR']


    write_csv( output_dir + "/countries.csv", countries, ['code', 'name'])

    write_csv(output_dir + "/ports.csv", ports, ['code', 'city', 'state'])

    create_mode(output_dir + "/modes.csv")

    create_visacodes(output_dir + "/visacodes.csv")

    create_visatypes(output_dir + "/visatypes.csv")

if __name__ == "__main__":
    main()
