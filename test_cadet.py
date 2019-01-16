import cadet as cadet
import click
from click.testing import CliRunner
import csv
from unittest import mock

import os
import pytest
import sys

GOOD_CSV_TEST_FILE = 'test.csv'
GOOD_TSV_TEST_FILE = 'test.tsv'

CSV_TYPE = 'CSV'
TSV_TYPE = 'TSV'

TXT_TEST_FILE = 'a.txt'

### Default Connection string, URI, Key, DB Name and Collection name are not actual instances of valid strings, currently used for testing purposes only
TEST_CONNECTION_STRING ='AccountEndpoint=https://testinguri.documents.azure.com:443/;AccountKey=testing==;'
TEST_URI = 'https://testinguri.documents.azure.com:443/'
TEST_KEY = 'testingkey=='

TEST_DB_NAME ='TestDB'
TEST_COLLECTION_NAME = 'TestCollection'
RUNNER = CliRunner()

class TestClass(object):
    # Tests that, given all required options, including a primary Key and URI combo and a CSV file, the tool works as expected
    @mock.patch('cadet.get_cosmos_client', autospec=True)
    def test_all_good_params_URI_primary_key_CSV(self, mock_get_cosmos_client):
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '-d', TEST_DB_NAME, '-c', TEST_COLLECTION_NAME, '-u', TEST_URI, '-k', TEST_KEY])
        assert '{"policyID": "172534", "statecode": "FL", "county": "CLAY COUNTY",' in result.output
        assert result.exit_code == 0

    # Tests that, given all required options, using a connection string and a CSV file, the tool works as expected
    @mock.patch('cadet.get_cosmos_client', autospec=True)
    def test_all_good_params_connection_string_CSV(self, mock_get_cosmos_client):
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '--connection-string', TEST_CONNECTION_STRING])
        assert '{"policyID": "172534", "statecode": "FL", "county": "CLAY COUNTY",' in result.output
        assert result.exit_code == 0

    # Tests that, given all required options, including a primary Key and URI combo and a TSV file, the tool works as expected
    @mock.patch('cadet.get_cosmos_client', autospec=True)
    def test_all_good_params_URI_primary_key_TSV(self, mock_get_cosmos_client):
        result = RUNNER.invoke(cadet.upload, [GOOD_TSV_TEST_FILE, '--type', TSV_TYPE, '-d', TEST_DB_NAME, '-c', TEST_COLLECTION_NAME, '-u', TEST_URI, '-k', TEST_KEY])
        assert '{"Name": "Zeke", "Age": "45", "Address": "W Main St"}' in result.output
        assert result.exit_code == 0

    # Tests that, given all required options, using a connection string and a TSV file, the tool works as expected
    @mock.patch('cadet.get_cosmos_client', autospec=True)
    def test_all_good_params_connection_string_TSV(self, mock_get_cosmos_client):
        result = RUNNER.invoke(cadet.upload, [GOOD_TSV_TEST_FILE, '--type', TSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '--connection-string', TEST_CONNECTION_STRING])
        assert '{"Name": "Zeke", "Age": "45", "Address": "W Main St"}' in result.output
        assert result.exit_code == 0

    # Tests that source file needs to be present
    def test_source_is_missing(self):
        result = RUNNER.invoke(cadet.upload, ['--type', CSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '-u', TEST_URI, '-k', TEST_KEY])
        assert result.exit_code != 0
        assert 'Missing argument "SOURCE"' in result.output

    # Tests that source file extension needs to be .csv or .tsv
    def test_source_file_txt(self):
        result = RUNNER.invoke(cadet.upload, [TXT_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '-u', TEST_URI, '-k', TEST_KEY])
        assert result.exit_code != 0
        assert 'We currently only support CSV and TSV uploads from Cadet' in result.output

    # Tests that DB name needs to be present
    def test_DB_not_present(self):
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE,'--collection-name', TEST_COLLECTION_NAME, '-u', TEST_URI, '-k', TEST_KEY])
        assert result.exit_code != 0
        assert 'Missing option "--database-name"' in result.output
    
    # Tests that collection name needs to be present
    def test_collection_not_present(self):
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB_NAME,  '-u', TEST_URI, '-k', TEST_KEY])
        assert result.exit_code != 0
        assert 'Missing option "--collection-name"' in result.output

    # Tests that connection string or primary key/URI needs to be present
    def test_connection_string_uri_and_primary_key_absence(self):
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME])
        assert result.exit_code != 0
        assert 'You must have a connection string OR *both* a URI and a key to use Cadet' in result.output

    # Tests that URI must be present, if connection string not present
    def test_URI_absent_key_present(self):
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '-k', TEST_KEY])
        assert result.exit_code != 0
        assert 'You must have a connection string OR *both* a URI and a key to use Cadet' in result.output        

    # Tests that primary key must be present if connection string not present
    def test_primary_key_absent_uri_present(self):
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '-u', TEST_URI])
        assert result.exit_code != 0
        assert 'You must have a connection string OR *both* a URI and a key to use Cadet' in result.output
    
    # Tests that connection string is correctly parsed
    def test_connection_string_parsing(self):
        invalid_uri = 'AccountEndpoint:invalidEndpoint'
        invalid_accountKey = 'AccountKey=invalidKey;'
        invalid_connection_string = invalid_uri + invalid_accountKey
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '-s', invalid_connection_string])
        assert result.exit_code != 0
        assert 'The connection string is not properly formatted - aborting' in result.output

    # Tests that an authentication failure raises an exception
    @mock.patch('cadet.get_cosmos_client', autospec=True)
    def test_authentication_failure(self, mock_get_cosmos_client):
            mock_get_cosmos_client.side_effect = click.BadParameter('Authentication failure to Azure Cosmos')
            result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '-d', TEST_DB_NAME, '-c', TEST_COLLECTION_NAME, '-u', TEST_URI, '-k', TEST_KEY])
            assert result.exit_code != 0
            assert 'Authentication failure to Azure Cosmos' in result.output