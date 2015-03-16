import sys, os, time, signal, threading, logging
import yaml, redis

class PidfileHelper(object):
    def __init__(self, pidfile):
        self._pidfile = pidfile
        
    def exist(self):
        """ Check for a pidfile to see if the daemon already runs """
        bExist = True
        try:
            with open(self._pidfile,'r') as pf:
                pid = int(pf.read().strip())
        except IOError:
            bExist = False
            
    def create(self):
        """ Create the pidfile """
        pid = str(os.getpid())
        with open(self._pidfile,'w+') as f:
            f.write(pid + '\n')
        return pid
        
    def delete(self):
        """ Remove the pidfile """
        os.remove(self._pidfile)

class BeholderConfig(object):
    def __init__(self, configFilename):
        with open(configFilename) as f:
            configData = yaml.safe_load(f)
            self.beholderLogFile = configData['beholder']['log_file']
            self.beholderConnectRetryCount = configData['beholder']['connect_retry_count']
            self.beholderConnectRetryInterval = configData['beholder']['connect_retry_interval']
            self.redisSentinelIP = configData['redis']['sentinel_ip']
            self.redisSentinelPort = configData['redis']['sentinel_port']
            self.twemproxyConfigFile = configData['twemproxy']['config_file']
            self.twemproxyRestartCommand = configData['twemproxy']['restart_command']
            
class BeholderLogger(object):
    def __init__(self, name, logFilename):
        # Create logger
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)
        # Create file handler
        handler = logging.FileHandler(logFilename)
        handler.setLevel(logging.INFO)
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # Configure logger
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)
        
    def debug(self, msg):
        self._logger.debug(msg)
        
    def info(self, msg):
        self._logger.info(msg)
        
    def warning(self, msg):
        self._logger.warning(msg) 
        
    def error(self, msg):
        self._logger.error(msg)
        
    def critical(self, msg):
        self._logger.critical(msg)
        
    def exception(self, msg):
        self._logger.exception(msg)
        
class Beholder(object):
    def __init__(self, pid, configFilename):
        self._config = BeholderConfig(configFilename)
        self._logger = BeholderLogger(pid, self._config.beholderLogFile)
        self._stopEvent = threading.Event()
        signal.signal(signal.SIGTERM, self._signalTerminationHandler)
        self._pubsub = None
        
    def __del__(self):
        if self._pubsub is not None:
            self._pubsub.close()
        
    def execute(self):
        if self._connect():
            while not self._stopEvent.is_set():
                message = self._pubsub.get_message()
                if message and message['type'] == 'message':
                    switchMasterInfo = str(message['data']).split(' ')
                    self._switchMaster(switchMasterInfo)
                time.sleep(0.001)
        self._logger.info('Beholder stopped')
        
    def _signalTerminationHandler(self, signaum, frame):
        """ Listen SIGTERM signal to stop the process in the right way """
        self._logger.info('Signal.SIGTERM received')
        self._stopEvent.set()
        
    def _connect(self):
        """ Connect to Redis sentinel and subscribe to the channel '+switch-master' to listen master changes """
        bConnected = False
        retryCount = 0
        while not self._stopEvent.is_set() and not bConnected:
            try:
                self._redis = redis.StrictRedis(host=self._config.redisSentinelIP, port=self._config.redisSentinelPort)
                self._pubsub = self._redis.pubsub()
                self._pubsub.subscribe(['+switch-master'])
            except Exception as e:
                self._logger.exception('Redis sentinel connection error: %s' % retryCount)
                # Check if maximum retry count is arrived
                if self._config.beholderConnectRetryCount > 0 and retryCount >= self._config.beholderConnectRetryCount:
                    self._stopEvent.set()
                    self._logger.critical('Max retries exceeded')
                else:
                    retryCount = retryCount + 1
                    time.sleep(self._config.beholderConnectRetryInterval / 1000)
            else:
                bConnected = True
            
        return bConnected
                
    def _switchMaster(self, switchMasterInfo):
        """ Called when new message is publish in the sentinel channel (+switch-master) """
        if len(switchMasterInfo)>3:
            oldIP = switchMasterInfo[1]
            oldPort = switchMasterInfo[2]
            newIP = switchMasterInfo[3]
            newPort = switchMasterInfo[4]
            if(self._updateMasters(oldIP, oldPort, newIP, newPort)):
                self._restartTwemproxy()
                self._logger.info('Master changed successfully')
            else:
                err = 'Master update error (%s:%s --> %s:%s)' % (oldIP, oldPort, newIP, newPort)
                self._logger.error(err)
        else:
            self._logger.warning('Wrong number of parameters: %s' % switchMasterInfo)

    def _updateMasters(self, oldServer, oldPort, newServer, newPort):
        """ Updates the address of a server in the twemproxy config """
        bUpdated = False
        
        try:
            with open(self._config.twemproxyConfigFile) as f:
                proxyData = yaml.safe_load(f)
        except:
            self._logger.exception('Could not open the twemproxy configuration file')
        else:
            for proxy in proxyData:
                for i, server in enumerate(proxyData[proxy]['servers']):
                    serverData = server.split(' ')
                    urlData = serverData[0].split(':')
                    
                    host = urlData[0]
                    port = urlData[1]
                    number = urlData[2]
                    name=''
                    
                    try:
                        name = serverData[1]
                    except:
                        pass
                        
                    # Check if the current server is the old master server
                    if str(host) == str(oldServer) and str(port) == str(oldPort):
                        host = newServer
                        port = newPort
                        proxyData[proxy]['servers'][i]= host + ':' + str(port) + ':' + str(number) + ' ' + name
                        self._logger.info('%s -> %s:%s changed to %s:%s' % (name, oldServer, oldPort, host, port))
                        bUpdated = True

            try:
                with open(self._config.twemproxyConfigFile, 'w') as f:
                    yaml.dump(proxyData, f, default_flow_style=False)
            except:
                self._logger.warning('Could not updated the twemproxy configuration file')
                bUpdated = False
                
            return bUpdated

    def _restartTwemproxy(self):
        """ Restart Twemproxy/Nutcracker service to reload the changes in configuration files """
        os.system(self._config.twemproxyRestartCommand)

if __name__ == "__main__":
    pidfile = sys.argv[1]  # '/var/run/beholder.pid'
    config = sys.argv[2]  # 'beholder.yml'
    
    pidfileHelper = PidfileHelper(pidfile)
    if not pidfileHelper.exist():
        try:
            pid = pidfileHelper.create()
            beholder = Beholder(pid, config)
            beholder.execute()
        finally:
            pidfileHelper.delete()
            sys.exit(0)
    else:
        sys.exist(1)