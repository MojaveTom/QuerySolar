#!/usr/bin/env python3

import time
import datetime
import os
import argparse
import sys
import configparser
import logging
import logging.config
import logging.handlers
import json
import binascii
from binascii import a2b_hex
import pymysql
import pymysql.err as Error
import paho.mqtt.publish as publish

import telnetlib
from telnetlib import DO, DONT, IAC, WILL, WONT, Telnet
import re

#######################  GLOBAL DEFINITIONS

# Configuration parameters without which we can do nothing.
RequiredConfigParams = frozenset((
    'inserter_host'
  , 'inserter_schema'
  , 'inserter_port'
  , 'inserter_user'
  , 'inserter_password'
  , 'mqtt_topic'
  , 'mqtt_host'
  , 'mqtt_port'
  , 'solar_host'
  , 'solar_port'
  , 'solar_table'
))

# GLOBALS
DBConn = None
dontWriteDb = True

SOLAR_WHLIFE   = b"whlife?\r"
SOLAR_KWHTODAY = b"kwhtoday?\r"
SOLAR_CUSTOM11 = b"custom11?\r"
SOLAR_MEASIN   = b"measin?\r"
SOLAR_MEASOUT  = b"measout?\r"
SOLAR_IDN      = b"idn?\r"
SOLAR_MODELID  = b"modelid?\r"

# Dictionary of messages to inverters and responses.
# Responses are extracted from return message by lambda function for each variable.
# Variable names MUST match database fields in the insert command; order is not important.
clientMessages = {  SOLAR_CUSTOM11: { 'Name': lambda m: m.replace(b'\r', b'').decode().strip()},
                    SOLAR_WHLIFE: {'LifeWattHour': lambda m: float(m)},
                    SOLAR_KWHTODAY: {'TodayWattHour': lambda m: float(m)*1000},
                    SOLAR_IDN: {'SerialNumber': lambda m: re.split(b'S:(.*)\r', m)[1].decode().strip(),
                        'Xid': lambda m: re.split(b'X:(.*?) ', m)[1].decode().strip(),
                        'ModelId': lambda m: re.split(b'M:(.*?) ', m)[1].decode().strip()},
                    SOLAR_MEASIN: {'InVoltsNow': lambda m: float(re.split(b'V:([0-9.]*) ?', m)[1]),
                        'InAmpsNow': lambda m: float(re.split(b'I:([0-9.]*) ?', m)[1]),
                        'InWattsNow': lambda m: float(re.split(b'P:([0-9.]*) ?', m)[1])},
                    SOLAR_MEASOUT: {'OutVoltsNow': lambda m: float(re.split(b'V:([0-9.]*) ?', m)[1]),
                        'OutAmpsNow': lambda m: float(re.split(b'I:([0-9.]*) ?', m)[1]),
                        'OutWattsNow': lambda m: float(re.split(b'P:([0-9.]*) ?', m)[1])}
                }
databaseQuery = """INSERT INTO `{schema}`.`{table}` 
(Name, TodayWattHour, LifeWattHour, SerialNumber, Xid, ModelId, 
InVoltsNow, InAmpsNow, InWattsNow, 
OutVoltsNow, OutAmpsNow, OutWattsNow) 
VALUES (%(Name)s, %(TodayWattHour)s, %(LifeWattHour)s, %(SerialNumber)s, %(Xid)s, %(ModelId)s, 
%(InVoltsNow)s, %(InAmpsNow)s, %(InWattsNow)s, 
%(OutVoltsNow)s, %(OutAmpsNow)s, %(OutWattsNow)s)"""


#####  Define logging
ProgFile = os.path.basename(sys.argv[0])
ProgName, ext = os.path.splitext(ProgFile)
ProgPath = os.path.dirname(os.path.realpath(sys.argv[0]))
logConfFileName = os.path.join(ProgPath, ProgName + '_loggingconf.json')
if os.path.isfile(logConfFileName):
    try:
        with open(logConfFileName, 'r') as logging_configuration_file:
            config_dict = json.load(logging_configuration_file)
        if 'log_file_path' in config_dict:
            logPath = os.path.expandvars(config_dict['log_file_path'])
            os.makedirs(logPath, exist_ok=True)
        else:
            logPath=""
        for p in config_dict['handlers'].keys():
            if 'filename' in config_dict['handlers'][p]:
                logFileName = os.path.join(logPath, config_dict['handlers'][p]['filename'])
                config_dict['handlers'][p]['filename'] = logFileName
        logging.config.dictConfig(config_dict)
    except Exception as e:
        print("loading logger config from file failed.")
        print(e)
        pass

logger = logging.getLogger(__name__)
logger.info('logger name is: "%s"', logger.name)

#  Generate a timezone for  LocalStandardTime
#  Leaving off zone name from timezone creator generates UTC based name which may be more meaningful.
localStandardTimeZone = datetime.timezone(-datetime.timedelta(seconds=time.timezone))
logger.debug('LocalStandardTime ZONE is: %s'%localStandardTimeZone)

####  LOCAL FUNCTIONS

def GetConfigFilePath():
    fp = os.path.join(ProgPath, 'secrets.ini')
    if not os.path.isfile(fp):
        fp = os.environ['PrivateConfig']
        if not os.path.isfile(fp):
            logger.error('No configuration file found: %s', fp)
            sys.exit(1)
    logger.info('Using configuration file at: %s', fp)
    return fp

def telnet_option_negotiation_cb(tsocket, command, option):
    """
    :param tsocket: telnet socket object
    :param command: telnet Command
    :param option: telnet option
    :return: None
    """
    if option == telnetlib.SGA:
        if command == DO:
            logger.debug("CB-send: IAC WILL SGA")
            tsocket.sendall(IAC + WILL + option)
        if command == DONT:
            logger.debug("CB-send: IAC WONT SGA")
            tsocket.sendall(IAC + WONT + option)
    elif option == telnetlib.BINARY:
        if command == DO:
            logger.debug("CB-send: IAC WILL BINARY")
            tsocket.sendall(IAC + WILL + option)
        if command == DONT:
            logger.debug("CB-send: IAC DONT BINARY")
            tsocket.sendall(IAC + WONT + option)
    elif command in (DO, DONT):
        logger.debug("CB-send: IAC WONT " + str(ord(option)))
        tsocket.sendall(IAC + WONT + option)
    elif command in (WILL, WONT):
        logger.debug("CB-send: IAC DONT " + str(ord(option)))
        tsocket.sendall(IAC + DONT + option)


##########################   MAIN
def main():

    global DBConn, dontWriteDb, localStandardTimeZone

    ## Determine the complete file paths for the config file and the graph definitions file.
    config = configparser.ConfigParser(interpolation=configparser.ExtendedInterpolation())
    configFile = GetConfigFilePath()
    # configFileDir = os.path.dirname(configFile)

    ##  Open up the configuration file and extract some parameters.
    config.read(configFile)
    cfgSection = ProgFile+"/"+os.environ['HOST']
    logger.info("INI file cofig section is: %s", cfgSection)
    # logger.debug('Config section has options: %s'%set(config.options(cfgSection)))
    # logger.debug('Required options are: %s'%RequiredConfigParams)
    if not config.has_section(cfgSection):
        logger.critical('Config file "%s", has no section "%s".', configFile, cfgSection)
        sys.exit(2)
    if len( RequiredConfigParams - set(config.options(cfgSection))) > 0:
        logger.critical('Config  section "%s" does not have all required params:\n"%s"\nit has params: "%s".', cfgSection, RequiredConfigParams, set(config.options(cfgSection)))
        logger.debug('The missing params are: %s'%(RequiredConfigParams - set(config.options(cfgSection)),))
        sys.exit(3)

    cfg = config[cfgSection]

    parser = argparse.ArgumentParser(description = 'Read data from solar inverter(s) and send to MQTT and database.')
    # parser.add_argument("-m","--meterId", dest="meterId", action="store", default=cfg['meter_id'], help="Numeric Id of EKM meter to read.")
    parser.add_argument("-r", "--repeatCount", dest="repeatCount", action="store", default='0', help="Number of times to read meinvertersters; 0 => forever.")
    parser.add_argument("-i", "--interval", dest="interval", action="store", default='15', help="The interval in munutes between successive inverter reads.")
    parser.add_argument("-W", "--dontWriteToDB", dest="noWriteDb", action="store_true", default=False, help="Don't write to database [during debug defaults to True].")
    parser.add_argument("-v", "--verbosity", dest="verbosity", action="count", help="increase output verbosity", default=0)
    args = parser.parse_args()
    # Verbosity = args.verbosity
    dontWriteDb = args.noWriteDb
    logger.debug('Write to DB? %s'%(not dontWriteDb))

    #  Prepare Solar parameters
    solarHost = cfg['solar_host']
    solarTable = cfg['solar_table']
    solarPorts = tuple(cfg['solar_port'].split())
    logger.debug('Solar parameters:  host="%s"; table="%s"; ports="%s"'%(solarHost, solarTable, solarPorts))

    #  Prepare MQTT parameters
    mqttTopic  = cfg['mqtt_topic']
    mqttPort   = int(cfg['mqtt_port'])
    mqttHost   = cfg['mqtt_host']

    ############  setup database connection
    user = cfg['inserter_user']
    pwd  = cfg['inserter_password']
    host = cfg['inserter_host']
    port = int(cfg['inserter_port'])
    schema = cfg['inserter_schema']
    logger.info("user %s"%(user,))
    logger.info("pwd %s"%(pwd,))
    logger.info("host %s"%(host,))
    logger.info("port %d"%(port,))
    logger.info("schema %s"%(schema,))

    #  Generate a timezone for  LocalStandardTime
    #  Leaving off zone name from timezone creator generates UTC based name which may be more meaningful.
    localStandardTimeZone = datetime.timezone(-datetime.timedelta(seconds=time.timezone))
    logger.debug('LocalStandardTime ZONE is: %s'%localStandardTimeZone)
    magicQuitPath = os.path.expandvars('${HOME}/.CloseQuerySolar')

    intervalSec = int(args.interval) * 60
    if intervalSec < 60:
        logger.warning('Looping intervals less than 1 minute not supported.  Set to 1 minute.')
        intervalSec = 60
    #### Don't sleep the first time through; just sample data now, then wait for next.
    # secSinceEpoch = time.time()
    # sleepLength = intervalSec - secSinceEpoch % intervalSec
    # logger.debug("Sleep for %s sec."%sleepLength)
    # time.sleep(sleepLength)
    # logger.debug('Slept for %s seconds.  It is now: %s'%(sleepLength, datetime.datetime.now().isoformat()))
    loopCount = int(args.repeatCount)
    if loopCount == 0: loopCount = 1000000000   #  Essentially keep going forever
    DBConn = pymysql.connect(host=host, port=port, user=user, password=pwd, database=schema, binary_prefix=True, charset='utf8mb4')
    logger.debug('DBConn is: %s'%DBConn)

    while loopCount > 0:
        try:
            for port in solarPorts:
                with DBConn.cursor() as cursor, Telnet("192.168.1.112", int(port), timeout=2) as tn:
                    tn.set_debuglevel(args.verbosity)
                    logger.debug('Got a telnet object: %s'%tn)
                    logger.debug('set option negotiator.')
                    tn.set_option_negotiation_callback(telnet_option_negotiation_cb)
                    logger.debug('Option negotiator is %s'%tn.option_callback)
                    tnSocket = tn.get_socket()
                    logger.debug('Connection socket: %s'%tnSocket)
                    time.sleep(.1)
                    #  Option negotiation startw when I read the telnet socket
                    while tn.sock_avail():
                        msg = tn.read_eager()
                        logger.debug('Received some data: %s'%msg)
                        time.sleep(.1)
                    #  Option negotiation completed
                    outputDict = {"ComputerTime": datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')}
                    for k, v in clientMessages.items():
                        logger.debug('Send a command: "%s"'%str(k))
                        tnSocket.sendall(k)
                        time.sleep(.1)
                        while tn.sock_avail():
                            msg = tn.read_eager()
                            logger.debug('Received some data: %s'%msg)
                            for n, f in v.items():
                                logger.debug('%s: %s'%(n, f(msg)))
                                outputDict[n] = f(msg)
                            time.sleep(.1)
                    outMsg = json.JSONEncoder().encode(outputDict)
                    logger.debug('Publishing meter data: "%s"'%outMsg)
                    publish.single(mqttTopic, payload = outMsg, hostname = mqttHost, port = mqttPort)
                    query = databaseQuery.format(schema = schema, table = solarTable)
                    logger.debug('Insertion query is: %s'%query)
                    if dontWriteDb:
                        logger.debug('NOT inserting into SolarEnergy table with query: "%s"'%cursor.mogrify(query, outputDict))
                    else:
                        logger.debug('Inserting into SolarEnergy table with query: "%s"'%cursor.mogrify(query, outputDict))
                        cursor.execute(query, outputDict)
                        DBConn.commit()
                    logger.debug('No more data availaible for this inverter port.')
        except pymysql.Error as e:
            logger.exception(e)
            time.sleep(10)
        else:
            # Only close connection when program ends
            pass

####              if magic shutdown file exists, exit loop, cleanup and exit
        sleepCounter = 0
        sleepLength = intervalSec - time.time() % intervalSec
        while sleepLength > 20:
            sleepCounter += 1
            time.sleep(20)
            if os.path.exists(magicQuitPath):
                logger.debug('Found magic quit file.')
                break       # break out of check magic file loop
            sleepLength = intervalSec - time.time() % intervalSec
        if os.path.exists(magicQuitPath):
            logger.debug('Quitting because magic file exists.')
            logger.debug('Delete magic file.')
            os.remove(magicQuitPath)
            break       #  break out of count loop
        sleepLength = intervalSec - time.time() % intervalSec
        logger.debug("Sleep for %s more sec."%sleepLength)
        time.sleep(sleepLength)
        logger.debug('Slept for a total of %s seconds.  It is now: %s'%(sleepLength + sleepCounter*20, datetime.datetime.now().isoformat()))

        loopCount = loopCount - 1
        if loopCount == 0:
            logger.debug('Loop counter exhausted.')
            break        #  No point in waiting if just going to quit
        else:
            logger.debug('Keep going %s more times.'%loopCount)

    logger.debug('Close the database connection.')
    DBConn.close()
    logger.info('             ##############   QuerySolar All Done   #################')

if __name__ == "__main__":
    main()
    pass
