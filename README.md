# Goal
The goal of the script in this repo is to solve a data analysis problem using Apache Spark, in this case PySpark. 

# Problem description
Detect which POS are recurrently out of stock.

Given the following source data:
1. List of transactions per point of sales (**transactions.csv**)
2. Stock level of every point of sale at some point in time (**stock-level.csv**)

For a POS to be considered **out of stock**, its stock balance needs to be lower than 150% of its average daily sales.

For a POS to be considered **recurrently** out of stock, the following conditions are required:
 - The POS has moved from with stock to out of stock at least twice 
 - It spent at least 4 hours out of stock

### Source data schema
#### Transactions
| Column             | Type      |
|--------------------|-----------|
| date               | timestamp |
| terminal_id        | text      |
| pos_id             | text      |
| transaction_amount | double    |
| stock_balance      | double    |

#### Stock level
| Column        | Type      |
|---------------|-----------|
| date          | timestamp |
| terminal_id   | text      |
| pos_id        | text      |
| stock_balance | double    |

Sample files are in the folder `data`

# Installation

1. Install Docker on the machine that will be used to run the code. Alternatively, the website 
https://labs.play-with-docker.com also works.

2. Get the project code using git:
```bash
git clone https://github.com/HugoNeves/Apache-Spark-Play
```

## Usage

From the root folder, run the command to build the Docker image
```bash
docker build -t spark_example .
```

Run the script inside the image.
```bash
docker run spark_example
```

It will use Spark, read the data, perform the necessary cleaning and transformations, 
and show on the console which POS are recurrently out of stock.