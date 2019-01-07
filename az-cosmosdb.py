#!/usr/bin/env python

import csv
import os
import click
import azure.cosmos.cosmos_client as cosmos_client

@click.command()
@click.option(
    '--endpoint', '-e',
    help="The endpoint URI for the CosmosDB instance"
    )
@click.option(
    '--conn-str', '-s',
    help="The connection string for the database"
    )
@click.option(
    '--database-name', '-d',
    help="The database name to connect to"
    )
@click.option(
    '--collection-name', '-c',
    help="The collection to load the data into"
    )
@click.option(
    '--throughput', '-t',
    help="The throughput value based on your Azure subscription ( 100 - 1 000 000 )")
@click.option(
    '--partition-key', '-k',
    help="The partition key for the data going in - think primary key")
@click.argument('source')
@click.option(
    '--source-type', '-y',
    help="The source file's type (Options: csv)"
)
def main(source, source_type, partition_key, collection_name, throughput, database_name, conn_str, endpoint):
    """
    Description blah blah

    """
    # Make sure it's a CSV or TSV
    source_type = source_type.upper()
    if source_type not in ["CSV", "TSV"]:
        print("WOMP")
        return

    delimiters = {
        "CSV": ',',
        "TSV": '\t'
    }

    config = {
        'ENDPOINT':endpoint,
        'PRIMARY_KEY': conn_str,
        'DATABASE': database_name,
        'CONTAINER': collection_name,
        'THROUGHPUT': throughput,
        'SOURCE_LOCATION': source,
        'PARTITION_KEY': partition_key
    }

    database_link = 'dbs/' + database_name
    collection_link = database_link + '/colls/' + collection_name

    client = cosmos_client.CosmosClient(
        url_connection=config['ENDPOINT'],
        auth={'masterKey': config['PRIMARY_KEY']}
        )

    #define the database / upsert?
    #set throughput
    options = {
        'offerThroughput': config['THROUGHPUT']
    }
    #define the container /upsert?

    #stats read for percentage done
    source_size = os.stat(config['SOURCE_LOCATION']).st_size
    print('Source total size is:', source_size, 'bytes')
    # click has something! look it up!

    #read and upload at same time
    with open(config['SOURCE_LOCATION'], 'r') as source_file:
        document = {}
        csv_reader = csv.reader(source_file, delimiter=delimiters[source_type])
        line_count = 0
        for row in csv_reader:
            if line_count == 0:
                csv_cols = row
                line_count += 1
            else:
                doc_vals = row
                for ind, col in enumerate(csv_cols):
                    document[csv_cols[ind]] = doc_vals[ind]
                print(document)
                result = client.UpsertItem(collection_link, document)
                print(result)

    # client.UpsertItem(CONTAINER_LINK,document, options=none)



    print("\nI'm a beautiful CLI ✨")

if __name__ == "__main__":
    main()
    