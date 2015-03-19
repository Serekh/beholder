import sys
import os
import time
import signal
import threading
import logging
import yaml
import redis


class PidfileHelper(object):
    def __init__(self, pidfile):
        self._pidfile = pidfile
        
    def exist(self):
        """ Check for a pidfile to see if the daemon already runs """
        b_exist = True
        try:
            with open(self._pidfile, 'r') as pf:
                pid = int(pf.read().strip())
        except IOError:
            b_exist = False
        return b_exist
            
    def create(self):
        """ Create the pidfile """
        pid = str(os.getpid())
        with open(self._pidfile, 'w+') as f:
            f.write(pid + '\n')
        return pid
        
    def delete(self):
        """ Remove the pidfile """
        os.remove(self._pidfile)


class BeholderConfig(object):
    def __init__(self, config_filename):
        with open(config_filename) as f:
            config_data = yaml.safe_load(f)
            self.beholder_log_file = config_data['beholder']['log_file']
            self.beholder_connect_retry_count = config_data['beholder']['connect_retry_count']
            self.beholder_connect_retry_interval = config_data['beholder']['connect_retry_interval']
            self.redis_sentinel_ip = config_data['redis']['sentinel_ip']
            self.redis_sentinel_port = config_data['redis']['sentinel_port']
            self.twemproxy_config_file = config_data['twemproxy']['config_file']
            self.twemproxy_restart_command = config_data['twemproxy']['restart_command']


class BeholderLogger(object):
    def __init__(self, name, log_filename):
        # Create logger
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)
        # Create file handler
        handler = logging.FileHandler(log_filename)
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
    def __init__(self, pid, config_filename):
        self._config = BeholderConfig(config_filename)
        self._logger = BeholderLogger(pid, self._config.beholder_log_file)
        self._stop_event = threading.Event()
        signal.signal(signal.SIGTERM, self._signal_termination_handler)
        self._pubsub = None
        
    def __del__(self):
        if self._pubsub is not None:
            self._pubsub.close()
        
    def execute(self):
        if self._connect():
            while not self._stop_event.is_set():
                message = self._pubsub.get_message()
                if message and message['type'] == 'message':
                    switch_master_info = str(message['data']).split(' ')
                    self._switch_master(switch_master_info)
                time.sleep(0.001)
        self._logger.info('Beholder stopped')
        
    def _signal_termination_handler(self, signaum, frame):
        """ Listen SIGTERM signal to stop the process in the right way """
        self._logger.info('Signal.SIGTERM received')
        self._stop_event.set()
        
    def _connect(self):
        """ Connect to Redis sentinel and subscribe to the channel '+switch-master' to listen master changes """
        b_connected = False
        retry_count = 0
        while not self._stop_event.is_set() and not b_connected:
            try:
                self._redis = redis.StrictRedis(host=self._config.redis_sentinel_ip, port=self._config.redis_sentinel_port)
                self._pubsub = self._redis.pubsub()
                self._pubsub.subscribe(['+switch-master'])
            except:
                self._logger.exception('Redis sentinel connection error: %s' % retry_count)
                # Check if maximum retry count is arrived
                if self._config.beholder_connect_retry_count > 0 and retry_count >= self._config.beholder_connect_retry_count:
                    self._stop_event.set()
                    self._logger.critical('Max retries exceeded')
                else:
                    retry_count += 1
                    time.sleep(self._config.beholder_connect_retry_interval / 1000)
            else:
                b_connected = True
            
        return b_connected
                
    def _switch_master(self, switch_master_info):
        """ Called when new message is publish in the sentinel channel (+switch-master) """
        if len(switch_master_info) > 3:
            old_ip = switch_master_info[1]
            old_port = switch_master_info[2]
            new_ip = switch_master_info[3]
            new_port = switch_master_info[4]
            if self._update_masters(old_ip, old_port, new_ip, new_port):
                self._restart_twemproxy()
                self._logger.info('Master changed successfully')
            else:
                err = 'Master update error (%s:%s --> %s:%s)' % (old_ip, old_port, new_ip, new_port)
                self._logger.error(err)
        else:
            self._logger.warning('Wrong number of parameters: %s' % switch_master_info)

    def _update_masters(self, old_server, old_port, new_server, new_port):
        """ Updates the address of a server in the twemproxy config """
        b_updated = False
        
        try:
            with open(self._config.twemproxy_config_file) as f:
                proxy_data = yaml.safe_load(f)
        except:
            self._logger.exception('Could not open the twemproxy configuration file')
        else:
            for proxy in proxy_data:
                for i, server in enumerate(proxy_data[proxy]['servers']):
                    server_data = server.split(' ')
                    url_data = server_data[0].split(':')
                    
                    host = url_data[0]
                    port = url_data[1]
                    number = url_data[2]
                    name = ''
                    
                    try:
                        name = server_data[1]
                    except:
                        pass
                        
                    # Check if the current server is the old master server
                    if str(host) == str(old_server) and str(port) == str(old_port):
                        host = new_server
                        port = new_port
                        proxy_data[proxy]['servers'][i]= host + ':' + str(port) + ':' + str(number) + ' ' + name
                        self._logger.info('%s -> %s:%s changed to %s:%s' % (name, old_server, old_port, host, port))
                        b_updated = True

            try:
                with open(self._config.twemproxy_config_file, 'w') as f:
                    yaml.dump(proxy_data, f, default_flow_style=False)
            except:
                self._logger.warning('Could not updated the twemproxy configuration file')
                b_updated = False
                
            return b_updated

    def _restart_twemproxy(self):
        """ Restart Twemproxy/Nutcracker service to reload the changes in configuration files """
        os.system(self._config.twemproxy_restart_command)

if __name__ == "__main__":
    pidfile = sys.argv[1]  # '/var/run/beholder.pid'
    config = sys.argv[2]  # 'beholder.yml'
    
    pidfile_helper = PidfileHelper(pidfile)
    if not pidfile_helper.exist():
        try:
            pid = pidfile_helper.create()
            beholder = Beholder(pid, config)
            beholder.execute()
        finally:
            pidfile_helper.delete()
            sys.exit(0)
    else:
        sys.exit(1)