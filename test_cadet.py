import cadet as cadet
import click
from click.testing import CliRunner
import csv
import mock
import os
import pytest
from pytest_mock import mocker
import sys


GOOD_CSV_TEST_FILE = 'test.csv'
GOOD_TSV_TEST_FILE = 'test.tsv'
CSV_TYPE = 'CSV'
TSV_TYPE = 'TSV'

TXT_TEST_FILE = 'a.txt'

### Remember to set these to environmental variables before running tests
VALID_CONNECTION_STRING = os.environ['AZURE_DB_CONNECTION_STRING']
VALID_URI = os.environ['AZURE_DB_URI']
VALID_KEY = os.environ['AZURE_DB_KEY']

VALID_DB_NAME = os.environ['AZURE_DB_NAME']
VALID_COLLECTION_NAME = os.environ['AZURE_DB_COLLECTION_NAME']

class TestClass(object):
    def test_all_good_params_URI_primary_key_CSV(self):
        runner = CliRunner()
        result = runner.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '-d', VALID_DB_NAME, '-c', VALID_COLLECTION_NAME, '-u', VALID_URI, '-k', VALID_KEY])
        assert result.exit_code == 0

    def test_all_good_params_connection_string_CSV(self):
        runner = CliRunner()
        result = runner.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', VALID_DB_NAME, '--collection-name', VALID_COLLECTION_NAME, '--connection-string', VALID_CONNECTION_STRING])
        assert result.exit_code == 0

    def test_all_good_params_URI_primary_key_TSV(self):
        runner = CliRunner()
        result = runner.invoke(cadet.upload, [GOOD_TSV_TEST_FILE, '--type', TSV_TYPE, '-d', VALID_DB_NAME, '-c', VALID_COLLECTION_NAME, '-u', VALID_URI, '-k', VALID_KEY])
        assert result.exit_code == 0

    def test_all_good_params_connection_string_TSV(self):
        runner = CliRunner()
        result = runner.invoke(cadet.upload, [GOOD_TSV_TEST_FILE, '--type', TSV_TYPE, '--database-name', VALID_DB_NAME, '--collection-name', VALID_COLLECTION_NAME, '--connection-string', VALID_CONNECTION_STRING])
        assert result.exit_code == 0

## Test that source must be present
    def test_source_is_missing(self):
        runner = CliRunner()
        result = runner.invoke(cadet.upload, ['--type', CSV_TYPE, '--database-name', VALID_DB_NAME, '--collection-name', VALID_COLLECTION_NAME, '-u', VALID_URI, '-k', VALID_KEY])
        assert result.exit_code != 0

## Test source is valid file type
    def test_source_file_txt(self):
        runner = CliRunner()
        result = runner.invoke(cadet.upload, [TXT_TEST_FILE, '--type', CSV_TYPE, '--database-name', VALID_DB_NAME, '--collection-name', VALID_COLLECTION_NAME, '-u', VALID_URI, '-k', VALID_KEY])
        assert result.exit_code != 0

## Test that database name must be valid and present
    def test_invalid_DB(self):
        runner = CliRunner()
        invalid_db_name = 'invalid_db'
        result = runner.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', invalid_db_name, '--collection-name', VALID_COLLECTION_NAME, '-u', VALID_URI, '-k', VALID_KEY])
        assert result.exit_code != 0   

    def test_DB_not_present(self):
        runner = CliRunner()
        result = runner.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE,'--collection-name', VALID_COLLECTION_NAME, '-u', VALID_URI, '-k', VALID_KEY])
        assert result.exit_code != 0   

## Test that collection name must be valid and present
    def test_invalid_collection(self):
        runner = CliRunner()
        invalid_collection_name = 'invalid_collection'
        result = runner.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', VALID_DB_NAME, '--collection-name', invalid_collection_name, '-u', VALID_URI, '-k', VALID_KEY])
        assert result.exit_code != 0   
    
    def test_collection_not_present(self):
        runner = CliRunner()
        result = runner.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', VALID_DB_NAME,  '-u', VALID_URI, '-k', VALID_KEY])
        assert result.exit_code != 0   

## Test that, if connection string not provided, endpoint AND primary key must be provided
    def test_connection_string_endpoint_and_primary_key_absence(self):
        runner = CliRunner()
        result = runner.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', VALID_DB_NAME, '--collection-name'])
        assert result.exit_code != 0  

    def test_URI_absent_key_present(self):
        runner = CliRunner()
        result = runner.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', VALID_DB_NAME, '--collection-name', VALID_COLLECTION_NAME, '-k', VALID_KEY])
        assert result.exit_code != 0  

    def test_primary_key_absent_uri_present(self):
        runner = CliRunner()
        result = runner.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', VALID_DB_NAME, '--collection-name', VALID_COLLECTION_NAME, '-u', VALID_URI])
        assert result.exit_code != 0

## Test that URI and key must be valid
    def test_invalid_URI(self):
        runner = CliRunner()
        invalid_URI = 'invalid_URI'
        result = runner.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', VALID_DB_NAME, '--collection-name', VALID_COLLECTION_NAME, '-u', invalid_URI, '-k', VALID_KEY])
        assert result.exit_code != 0
        

    def test_invalid_key(self):
        runner = CliRunner()
        invalid_key = 'invalid_key'
        result = runner.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', VALID_DB_NAME, '--collection-name', VALID_COLLECTION_NAME, '-u', VALID_URI, '-k', invalid_key])
        assert result.exit_code != 0

## Test that AccountEndpoint/AccountKey are parsed as they should be
    def test_connection_string_present(self):
        runner = CliRunner()
        invalid_endpoint = 'AccountEndpoint=invalidEndpoint;'
        invalid_accountKey = 'AccountKey=invalidKey;'
        invalid_connection_string = invalid_endpoint + invalid_accountKey
        result = runner.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', VALID_DB_NAME, '--collection-name', VALID_COLLECTION_NAME, '-s', invalid_connection_string])
        assert result.exit_code != 0
        