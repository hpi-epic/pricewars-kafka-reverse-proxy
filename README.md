# Kafka Reverse Proxy

This repository contains the Kafka Reverse Proxy which acts as an interfacing component between our Kafka streams and merchants.
Data-driven merchants have to be able to retrieve past market situations written to Kafka to do their learning. However, they should only be allowed to access the market situation data that they would have access to on a real system as well, i.e., the offers that were on the marketplace at a certain time and only their own sales data.

This service provides the data as CSV files.

The meta repository containing general information can be found [here](https://github.com/hpi-epic/pricewars)

## Application Overview

| Repo |
|--- |
| [UI](https://github.com/hpi-epic/pricewars-mgmt-ui) |
| [Consumer](https://github.com/hpi-epic/pricewars-consumer) |
| [Producer](https://github.com/hpi-epic/pricewars-producer) |
| [Marketplace](https://github.com/hpi-epic/pricewars-marketplace) |
| [Merchant](https://github.com/hpi-epic/pricewars-merchant) |
| [Kafka RESTful API](https://github.com/hpi-epic/pricewars-kafka-rest) |

## Requirements
* Python 3.5 or higher

## Setup

After cloning the repository, install the necessary dependencies with:

```python3 -m pip install -r requirements.txt```

Then start the kafka proxy by running

```python3 LoggerApp.py --kafka-url <KAFKA_URL>```

The LoggerApp will run on http://localhost:8001.

Furthermore, it is advisable to delete (old) files in the data folder which stores the learning CSV files for the data-driven merchants.

## Interfaces

### Filtered data view as CSV

For merchants to view past market situations and use them for training a model, the kafka reverse proxy offers the export of this data as a csv-file:

![](docs/rest_topic.png)

#### Request data export

The data export can be triggered using a GET-request on the _/export/data_-route. The export expects a merchant_token in the _Authorization_-header so that the exported data only includes the data visible to the merchant belonging to that token.
Currently accessible topics are `buyOffer` and `marketSituation`.

```
HTTP GET export/data/<topic>
```

generates a CSV file for the given merchant_token and returns the path as json:

```
{"url": "path/to/file.csv"}
```

#### Receiving CSV file

To retrieve the CSV file, do a GET-request on the returned path:

```HTTP GET data/dump.csv```
