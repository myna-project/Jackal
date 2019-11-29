# Jackal
Jackal is a modular python parser and [IEnergyDa](https://github.com/myna-project/IEnergyDa) REST client.
It has been developed to address the specific need of parsing text files from various energy gateways and data loggers and send the parsed data in JSON format via HTTP(s) to the IEnergyDa backend.
It has been released as an open-source project in November 2019.

Jackal can parse:
* the XML containing the monthly energy consumption of a POD provided by italian energy distributors
* the CSV output of the following devices:
* [Schneider PowerLogic EGX300 Gateway](https://www.se.com/ww/en/product-range-presentation/2333-powerlogic-egx300/), tested with the following meter:
  * Schneider PowerLogic PM5100
  * Schneider Micrologic
  * Schneider iEM3150
  * Schneider iEM3250
  * Schneider iEM3350
* [Solar-Log](https://www.solar-log.com/en/products-components/monitoring-solar-logTM/?L=44%2F), tested with the following models:
  * 1000
  * 2000

Jackal can also parse the CSV files made available from the Italian energy distributor [Deval](http://www.devalspa.it/) through his website.
### Installation requirements
Jackal is compatible with **python3** (developed and tested with **3.7**). With **2.7** it "might works".
Jackal requires the following python modules:
* dateutil
* lxml
* pyinotify
* requests
* schedule
* tzlocal

For installation on Debian and derivative distros (eg. Ubuntu) you can use apt:
```
apt-get install python3-dateutil python3-lxml python3-pyinotify python3-requests python3-schedule python3-tzlocal
```
Otherwise you can use pip:
```
pip install dateutil lxml pyinotify requests schedule tzlocal
```
Jackal relies on [systemd](https://github.com/systemd/systemd) to behave like a daemon and restart in case of failures. The systemd unit for Jackal is provided. Our reference distro is **Debian**, anyway the same results can be achieved with other service managers such as [supervisor](https://github.com/Supervisor/supervisor).
### Plugins development
Jackal architecture is modular and through for rapid development of new plugins. Jackal searches and loads plugins in the plugins directory and use them if they are configured in the ini configuration file. Plugins are simply classes with a **parse()** method that receives in input a buffer with the content of the entire text file (CSV, XML, etc.) and outputs a python dict with a standard structure (a client id, timestamp, a device id and the measures in object form {measure_id, value}).
