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

DELIMITERS = {
    'CSV': ',',
    'TSV': '\t'
}


@click.version_option('1.0.0')
@click.group()
def cadet():
    """
    Acts as a super-group for the import command and any future commands to cadet
    """
    pass


@cadet.command(name='import')
@click.option(
    '--connection-string', '-s',
    help='The connection string for the database'
    )
@click.option(
    '--uri', '-u',
    help='The endpoint URI for the CosmosDB instance'
    )
@click.option(
    '--primary-key', '-k', '--key',
    help='The key provided to access the CosmosDB database'
)
@click.option(
    '--database-name', '-d', '--database',
    help='The name of the database to connect to',
    required=True
    )
@click.option(
    '--collection-name', '-c', '--collection',
    help='The collection to load the data into',
    required=True
    )
@click.option(
    '--type', '-t', 'type_',
    help='The source file\'s type (Options: csv, tsv)',
    required=True
)
@click.argument('source')
def upload(source, type_, collection_name, database_name, primary_key, uri, connection_string):
    """
    Given a source file `source` of type `type_`:
        1. connects to the Cosmos DB instance using either
            (a) `primary_key` and `uri`
            OR
            (b) `connection_string`
        2. ...and uploads the data to the collection `collection_name` on database `database_name

    Assumes that the Cosmos DB subscription has both the database and the collection already
    made when running this tool.
    """
    # Make sure it's a CSV or TSV
    type_ = type_.upper()
    if type_ not in ['CSV', 'TSV'] or not source.upper().endswith(('.CSV', '.TSV')):
        raise click.BadParameter('We currently only support CSV and TSV uploads from Cadet')

    # You must have either the connection string OR (endpoint and key) to connect
    if (uri is None or primary_key is None) and (connection_string is None):
        raise click.BadParameter(
            'REQUIRED: Connection string OR *both* a URI and a key'
            )
    elif uri is not None and primary_key is not None:
        _connection_url = uri
        _auth = {'masterKey': primary_key}
    elif connection_string is not None:
        connection_str = connection_string.split(';')
        _connection_url = connection_str[0].replace('AccountEndpoint=', '')

        try:
            # If someone provides the connection string, break it apart into its subcomponents
            if 'AccountEndpoint=' not in connection_string or 'AccountKey=' not in connection_string:
                raise click.BadParameter('The connection string is not properly formatted.')
            connection_str = connection_string.split(';')
            _connection_url = connection_str[0].replace('AccountEndpoint=', '')
            _auth = {'masterKey': connection_str[1].replace('AccountKey=', '')}
        except:
            # ...Unless they don't provide a usable connection string
            raise click.BadParameter('The connection string is not properly formatted.')

    database_link = 'dbs/' + database_name
    collection_link = database_link + '/colls/' + collection_name

    # Connect to Cosmos
    try:
        client = get_cosmos_client(_connection_url, _auth)
    except:
        raise click.BadParameter('Authentication failure to Azure Cosmos')

    # Read and upload at same time
    try:
        source_path = get_full_source_path(source)
        read_and_upload(source_path, type_, client, collection_link)
    except FileNotFoundError as err:
        raise click.FileError(source, hint=err)

def get_full_source_path(source):
    return os.path.join(os.path.dirname(__file__), source)

def get_cosmos_client(connection_url, auth):
    """
    Connects to the Cosmos instance via the `connection_url` (authenticating with `auth`)
    and returns the cosmos_client
    """
    return cosmos_client.CosmosClient(
        url_connection=connection_url,
        auth=auth
    )


def read_and_upload(source_path, file_type, client, collection_link):
    """
    Reads the CSV `source` of type `file_type`, connects to the `client` to the
    database-collection combination found in `collection_link`
    """
     # Stats read for percentage done
    source_size = os.stat(source_path).st_size
    click.echo('Source file total size is: %s bytes' % source_size)
    with open(source_path, 'r') as source_file:
        click.echo('Starting the upload')
        document = {}
        csv_reader = csv.reader(source_file, delimiter=DELIMITERS[file_type])
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


if __name__ == '__main__':
    cadet()
