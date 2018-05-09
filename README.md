# OOI Data Download
This toolbox contains an example of how to download data from uFrame via the OOI Machine to Machine (M2M) interface. Data can be downloaded from multiple platforms, sensors, and data streams.

- Author: Lori Garzio - Rutgers University

### Installation
    > git clone https://github.com/ooi-data-review/data_download.git
    > cd data_download
    > pip install .
    > pip install -r requirements.txt

### Scripts
- [request_data_m2m.py](https://github.com/ooi-data-review/data_download/blob/master/request_data_m2m.py): Imports tools that compare the [OOI Datateam Database](http://ooi.visualocean.net/) to [ooinet.oceanobservatories.org](https://ooinet.oceanobservatories.org/), builds netCDF data request urls, sends the requests, and writes a summary of the status of the data requests. This interactive tool prompts the user for inputs.

### Tools
- [data_request_tools.py](https://github.com/ooi-data-review/data_download/blob/master/tools/data_request_tools.py): Format and filter information

- [get_data_request_urls.py](https://github.com/ooi-data-review/data_download/blob/master/tools/get_data_request_urls.py): Compares the reference designators, methods and streams in the QC database to those available in the GUI data catalog ('https://ooinet.oceanobservatories.org/api/uframe/stream'), and build data request urls (for netCDF files) for the science streams of the instruments input by the user

- [send_data_requests_nc.py](https://github.com/ooi-data-review/data_download/blob/master/tools/send_data_requests_nc.py): Sends data requests for all urls contained in the output from [get_data_request_urls.py](https://github.com/ooi-data-review/data_download/blob/master/tools/get_data_request_urls.py) and provides a summary output that contains the links to the THREDDS data server.

### Notes
- In order to access OOI data through the uFrame API, you will need to create a user account on [ooinet.oceanobservatories.org](https://ooinet.oceanobservatories.org/). Your API Username and Token can be found in your User Profile.