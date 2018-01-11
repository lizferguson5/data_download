# OOI Data Download
This toolbox is used by the OOI Data Review Team at Rutgers University in order to download data from uFrame via the Machine to Machine (M2M) interface.

### Scripts
- [request_data_m2m.py](https://github.com/ooi-data-review/data_download/blob/master/request_data_m2m.py): Imports tools that compare the QC Database (http://ooi.visualocean.net/) to the OOI GUI data catalog (https://ooinet.oceanobservatories.org/), builds netCDF data request urls, sends the requests, and writes a summary of the status of the data requests. This interactive tool prompts the user for inputs.