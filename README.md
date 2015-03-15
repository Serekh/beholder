# Beholder

Beholder is an agent written in [Python](https://www.python.org/) for [twemproxy](https://github.com/twitter/twemproxy) to work with a  [redis sentinel](http://redis.io/topics/sentinel) to provide support for failover.

Beholder's purpose is to extend the HA capabilities of twemproxy even after a redis node has failed.

In order to accomplish this, beholder is continuously monitoring the **+switch-master** event, then it will update twemproxy configuration files and restart the resource.

## Dependencies

Beholder use [redis-py](https://github.com/andymccurdy/redis-py) and [pyyaml](http://pyyaml.org/), so the first step is install the dependencies:

    $ pip install redis
	$ pip install pyyaml
	
If you want to install python yaml system-wide in linux, you can also use a package manager, like:

	$ sudo apt-get install python-yaml
	$ sudo yum install python-yaml
	
## Installation

The follow installation steps are the default ones, feel free to change any path.

	$ sudo cp src/beholder.py /usr/local/bin/beholder.py
	$ sudo cp config/beholder.yml /etc/nutcracker/beholder.yml
	$ sudo cp scripts/beholder.init /etc/init.d/beholder
	$ sudo chmod 775 /etc/init.d/beholder
	$ sudo chkconfig --add /etc/init.d/beholder
	$ sudo chkconfig beholder on
	$ sudo service beholder start

## Configuration

Beholder can be configured through a YAML file [beholder.yml](config/beholder.yml).

    beholder:
      log_file: "/var/log/beholder.log"
      connect_retry_count: -1 # l-1 to unlimited
      connect_retry_interval: 1000 # Milliseconds

    redis:
      sentinel_ip: "127.0.0.1"
      sentinel_port: 26379

    twemproxy:
      config_file: "/etc/nutcracker/nutcracker.yml"
      restart_command: "/etc/init.d/nutcracker restart"
	  
## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request	  
	  
## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0

See [LICENSE.md](LICENSE)