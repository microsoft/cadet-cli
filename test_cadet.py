import cadet as cadet
import click
from click.testing import CliRunner
import csv
from mock import Mock
import os
import pytest
import pytest_mock
from pytest_mock import mocker
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
    def test_all_good_params_URI_primary_key_CSV(self):
        mock_testing = Mock()
        mock_testing.UpsertItem.return_value = {}
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '-d', TEST_DB_NAME, '-c', TEST_COLLECTION_NAME, '-u', TEST_URI, '-k', TEST_KEY, '--testing', mock_testing])
        assert result.exit_code == 0

    def test_all_good_params_connection_string_CSV(self):
        mock_testing = Mock()
        mock_testing.UpsertItem.return_value = {}
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '--connection-string', TEST_CONNECTION_STRING, '--testing', mock_testing])
        assert result.exit_code == 0

    def test_all_good_params_URI_primary_key_TSV(self):
        mock_testing = Mock()
        mock_testing.UpsertItem.return_value = {}
        result = RUNNER.invoke(cadet.upload, [GOOD_TSV_TEST_FILE, '--type', TSV_TYPE, '-d', TEST_DB_NAME, '-c', TEST_COLLECTION_NAME, '-u', TEST_URI, '-k', TEST_KEY, '--testing', mock_testing])
        assert result.exit_code == 0

    def test_all_good_params_connection_string_TSV(self):
        mock_testing = Mock()
        mock_testing.UpsertItem.return_value = {}
        result = RUNNER.invoke(cadet.upload, [GOOD_TSV_TEST_FILE, '--type', TSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '--connection-string', TEST_CONNECTION_STRING, '--testing', mock_testing])
        assert result.exit_code == 0

    def test_source_is_missing(self):
        mock_testing = Mock()
        mock_testing.UpsertItem.return_value = {}
        result = RUNNER.invoke(cadet.upload, ['--type', CSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '-u', TEST_URI, '-k', TEST_KEY, '--testing', mock_testing])
        assert result.exit_code != 0
        assert 'Missing argument "SOURCE"' in result.output

    def test_source_file_txt(self):
        mock_testing = Mock()
        mock_testing.UpsertItem.return_value = {}
        result = RUNNER.invoke(cadet.upload, [TXT_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '-u', TEST_URI, '-k', TEST_KEY, '--testing', mock_testing])
        assert result.exit_code != 0
        assert 'We currently only support CSV and TSV uploads from Cadet' in result.output

    def test_DB_not_present(self):
        mock_testing = Mock()
        mock_testing.UpsertItem.return_value = {}
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE,'--collection-name', TEST_COLLECTION_NAME, '-u', TEST_URI, '-k', TEST_KEY, '--testing', mock_testing])
        assert result.exit_code != 0
        assert 'Missing option "--database-name"' in result.output
    
    def test_collection_not_present(self):
        mock_testing = Mock()
        mock_testing.UpsertItem.return_value = {}
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB_NAME,  '-u', TEST_URI, '-k', TEST_KEY, '--testing', mock_testing])
        assert result.exit_code != 0
        assert 'Missing option "--collection-name"' in result.output

    def test_connection_string_uri_and_primary_key_absence(self):
        mock_testing = Mock()
        mock_testing.UpsertItem.return_value = {}
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '--testing', mock_testing])
        assert result.exit_code != 0
        assert 'You must have a connection string OR *both* a URI and a key to use Cadet' in result.output

    def test_URI_absent_key_present(self):
        mock_testing = Mock()
        mock_testing.UpsertItem.return_value = {}
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '-k', TEST_KEY, '--testing', mock_testing])
        assert result.exit_code != 0
        assert 'You must have a connection string OR *both* a URI and a key to use Cadet' in result.output        

    def test_primary_key_absent_uri_present(self):
        mock_testing = Mock()
        mock_testing.UpsertItem.return_value = {}
        result = RUNNER.invoke(cadet.upload, [GOOD_CSV_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB_NAME, '--collection-name', TEST_COLLECTION_NAME, '-u', TEST_URI, '--testing', mock_testing])
        assert result.exit_code != 0
        assert 'You must have a connection string OR *both* a URI and a key to use Cadet' in result.output
