#!/usr/bin/env python
"""
This CLI tool allows a user to upload CSV and TSV files to an Azure CosmosDB instance configured
to use DocumentDB.
"""

import csv
import os
import sys
import click
import azure.cosmos.cosmos_client as cosmos_client


@click.group()
def main():
    """
    Acts as a super-group for the import command and any future commands to cadet
    """
    pass


@main.command(name='import')
@click.option(
    '--connection-string', '-s',
    help='The connection string for the database'
    )
@click.option(
    '--uri', '-u',
    help='The endpoint URI for the CosmosDB instance'
    )
@click.option(
    '--primary-key', '-k',
    help='The key provided to access the CosmosDB database'
)
@click.option(
    '--database-name', '-d',
    help='The name of the database to connect to'
    )
@click.option(
    '--collection-name', '-c',
    help='The collection to load the data into'
    )
@click.option(
    '--type', '-t', 'type_',
    help='The source file\'s type (Options: csv, tsv)'
)
@click.argument('source')
def upload(source, type_, collection_name, database_name, primary_key, uri, connection_string):
    """
    Assumes that the Cosmos DB subscription has both the database and the collection already
    made when running this tool.
    """
    # Make sure it's a CSV or TSV
    type_ = type_.upper()
    if type_ not in ['CSV', 'TSV'] or not source.upper().endswith(('.CSV', '.TSV')):
        raise click.BadParameter('We currently only support CSV and TSV uploads from Cadet')

    delimiters = {
        'CSV': ',',
        'TSV': '\t'
    }

    # You must have either the connection string OR (endpoint and key) to connect
    if (uri is None or primary_key is None) and (connection_string is None):
        raise click.BadParameter(
            'You must have a connection string OR *both* a URI and a key to use Cadet'
            )
    elif uri is not None and primary_key is not None:
        _connection_url = uri
        _auth = {'masterKey': primary_key}
    elif connection_string is not None:
        try:
            # If someone provides the connection string, break it apart into its subcomponents
            conn_str = connection_string.split(';')
            _connection_url = conn_str[0].replace('AccountEndpoint=', '')
            _auth = {'masterKey': conn_str[1].replace('AccountKey', '')}
        except:
            # ...Unless they don't provide a usable connection string
            raise click.BadParameter('The connection string is not properly formatted - aborting')


    database_link = 'dbs/' + database_name
    collection_link = database_link + '/colls/' + collection_name

    try:
        client = cosmos_client.CosmosClient(
            url_connection=_connection_url,
            auth=_auth
            )
    except:
        raise click.BadParameter('Authentication failure to Azure Cosmos')

    # Read and upload at same time
    try:
        # Stats read for percentage done
        source_size = os.stat(source).st_size
        click.echo('Source file total size is: %s bytes' % source_size)

        with open(source, 'r') as source_file:
            click.echo('Starting the upload')
            document = {}
            csv_reader = csv.reader(source_file, delimiter=delimiters[type_])
            line_count = 0
            with click.progressbar(length=source_size, show_percent=True) as status_bar:
                for row in csv_reader:
                    if line_count == 0:
                        csv_cols = row
                        line_count += 1
                    else:
                        for ind, col in enumerate(csv_cols):
                            document[col] = row[ind]

                        try:
                            client.UpsertItem(collection_link, document)
                            status_bar.update(sys.getsizeof(document))
                        except:
                            raise click.ClickException('Upload failed')
        click.echo('Upload complete!')
    except FileNotFoundError as e:
        raise click.FileError(source, hint=e)

if __name__ == '__main__':
    main()
