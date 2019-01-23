import copy
from unittest import mock

from click.testing import CliRunner
from requests.exceptions import ConnectionError

from ..cadet import (
    upload
)

GOOD_CSV = 'test.csv'
GOOD_TSV = 'test.tsv'

CSV_TYPE = 'CSV'
TSV_TYPE = 'TSV'

TXT_TEST_FILE = 'a.txt'

# Default Connection string, URI, Key, DB Name and Collection name are
# not actual instances of valid strings; currently used for testing purposes only
TEST_CONN_STRING = 'AccountEndpoint=https://testinguri.com:443/;AccountKey=testing==;'
TEST_URI = 'https://testinguri.documents.azure.com:443/'
TEST_KEY = 'testingkey=='

TEST_DB = 'TestDB'
TEST_COLLECTION = 'TestCollection'
RUNNER = CliRunner()

class MockClient(object):
    def __init__(self):
        self.upserted_docs = list()

    def UpsertItem(self, collectionLink, document):
        self.upserted_docs.append(copy.copy(document))

    def CosmosClient(self, url_connection, auth):
        raise ConnectionError('Authentication failure to Azure Cosmos')

class TestClass(object):
    # Tests that, given all required options, including a primary Key and URI combo
    # and a CSV file, the tool works as expected
    @mock.patch('src.cadet.get_cosmos_client', autospec=True)
    def test_all_good_params_uri_primary_key_csv(self, mock_get_cosmos_client):
        MC = MockClient()
        mock_get_cosmos_client.return_value = MC
        result = RUNNER.invoke(
            upload, [GOOD_CSV, '--type', CSV_TYPE, '-d', TEST_DB,
                     '-c', TEST_COLLECTION, '-u', TEST_URI, '-k', TEST_KEY]
            )

        expected_keys = ['county', 'eq_site_limit', 'policyID', 'statecode']

        expected_values = list()

        # Expected values from the test.csv file
        expected_values.append(['119736', '498960', 'CLAY COUNTY', 'FL'])
        expected_values.append(['1322376', '448094', 'CLAY COUNTY', 'FL'])
        expected_values.append(['190724', '206893', 'CLAY COUNTY', 'FL'])
        expected_values.append(['0', '333743', 'CLAY COUNTY', 'FL'])
        expected_values.append(['0', '172534', 'CLAY COUNTY', 'FL'])

        # Collection assert that expected values and actual upserted values are equal
        for num in range(len(MC.upserted_docs)):
            vals = MC.upserted_docs[num]

            keys = list(vals.keys())
            vals = list(vals.values())

            keys.sort()
            vals.sort()
            assert keys == expected_keys
            assert vals == expected_values[num]

        assert len(MC.upserted_docs) == 5
        assert result.exit_code == 0

    # Tests that, given all required options, using a connection string and a CSV file,
    # the tool works as expected
    @mock.patch('src.cadet.get_cosmos_client', autospec=True)
    def test_all_good_params_connection_string_csv(self, mock_get_cosmos_client):
        MC = MockClient()
        mock_get_cosmos_client.return_value = MC
        result = RUNNER.invoke(
            upload, [GOOD_CSV, '--type', CSV_TYPE, '--database-name', TEST_DB,
                     '--collection-name', TEST_COLLECTION, '--connection-string', TEST_CONN_STRING]
            )

        expected_keys = ['county', 'eq_site_limit', 'policyID', 'statecode']

        # Expected keys/headers from the test.csv file
        expected_values = list()

        # Expected values from the test.csv file
        expected_values.append(['119736', '498960', 'CLAY COUNTY', 'FL'])
        expected_values.append(['1322376', '448094', 'CLAY COUNTY', 'FL'])
        expected_values.append(['190724', '206893', 'CLAY COUNTY', 'FL'])
        expected_values.append(['0', '333743', 'CLAY COUNTY', 'FL'])
        expected_values.append(['0', '172534', 'CLAY COUNTY', 'FL'])

        # Collection assert that expected values and actual upserted values are equal
        for num in range(len(MC.upserted_docs)):
            vals = MC.upserted_docs[num]

            keys = list(vals.keys())
            vals = list(vals.values())

            keys.sort()
            vals.sort()
            assert keys == expected_keys
            assert vals == expected_values[num]

        assert len(MC.upserted_docs) == 5
        assert result.exit_code == 0


    # Tests that, given all required options, including a primary Key and URI combo
    # and a TSV file, the tool works as expected
    @mock.patch('src.cadet.get_cosmos_client', autospec=True)
    def test_all_good_params_uri_primary_key_tsv(self, mock_get_cosmos_client):
        MC = MockClient()
        mock_get_cosmos_client.return_value = MC
        result = RUNNER.invoke(
            upload, [GOOD_TSV, '--type', TSV_TYPE, '-d', TEST_DB,
                     '-c', TEST_COLLECTION, '-u', TEST_URI, '-k', TEST_KEY]
            )
        # Expected keys/headers from the test.csv file
        expected_keys = ['Address', 'Age', 'Name']

        expected_values = list()

        # Expected values from the test.csv file
        expected_values.append(['1115 W Franklin', '23', 'Paul'])
        expected_values.append(['5', 'Bessy the Cow', 'Big Farm Way'])
        expected_values.append(['45', 'W Main St', 'Zeke'])

        # Collection assert that expected values and actual upserted values are equal
        for num in range(len(MC.upserted_docs)):
            vals = MC.upserted_docs[num]

            keys = list(vals.keys())
            vals = list(vals.values())

            keys.sort()
            vals.sort()
            assert keys == expected_keys
            assert vals == expected_values[num]

        assert len(MC.upserted_docs) == 3
        assert result.exit_code == 0

    # Tests that, given all required options, using a connection string and a TSV file,
    # the tool works as expected
    @mock.patch('src.cadet.get_cosmos_client', autospec=True)
    def test_all_good_params_connection_string_tsv(self, mock_get_cosmos_client):
        MC = MockClient()
        mock_get_cosmos_client.return_value = MC
        result = RUNNER.invoke(
            upload, [GOOD_TSV, '--type', TSV_TYPE, '--database-name', TEST_DB,
                     '--collection-name', TEST_COLLECTION, '--connection-string', TEST_CONN_STRING]
            )

        # Expected keys/headers from the test.csv file
        expected_keys = ['Address', 'Age', 'Name']

        expected_values = list()

        # Expected values from the test.csv file
        expected_values.append(['1115 W Franklin', '23', 'Paul'])
        expected_values.append(['5', 'Bessy the Cow', 'Big Farm Way'])
        expected_values.append(['45', 'W Main St', 'Zeke'])

        # Collection assert that expected values and actual upserted values are equal
        for num in range(len(MC.upserted_docs)):
            vals = MC.upserted_docs[num]

            keys = list(vals.keys())
            vals = list(vals.values())

            keys.sort()
            vals.sort()
            assert keys == expected_keys
            assert vals == expected_values[num]

        assert len(MC.upserted_docs) == 3
        assert result.exit_code == 0

    # Tests that source file needs to be present
    def test_source_is_missing(self):
        result = RUNNER.invoke(
            upload, ['--type', CSV_TYPE, '--database-name', TEST_DB,
                     '--collection-name', TEST_COLLECTION, '-u', TEST_URI, '-k', TEST_KEY]
            )
        assert result.exit_code != 0
        assert 'Missing argument "SOURCE"' in result.output

    # Tests that source file extension needs to be .csv or .tsv
    def test_source_file_txt(self):
        result = RUNNER.invoke(
            upload, [TXT_TEST_FILE, '--type', CSV_TYPE, '--database-name', TEST_DB,
                     '--collection-name', TEST_COLLECTION, '-u', TEST_URI, '-k', TEST_KEY]
            )
        assert result.exit_code != 0
        assert 'We currently only support CSV and TSV uploads from Cadet' in result.output

    # Tests that DB name needs to be present
    def test_db_not_present(self):
        result = RUNNER.invoke(
            upload, [GOOD_CSV, '--type', CSV_TYPE, '--collection-name', TEST_COLLECTION,
                     '-u', TEST_URI, '-k', TEST_KEY]
            )
        assert result.exit_code != 0
        assert 'Missing option "--database-name"' in result.output

    # Tests that collection name needs to be present
    def test_collection_not_present(self):
        result = RUNNER.invoke(
            upload, [GOOD_CSV, '--type', CSV_TYPE, '--database-name',
                     TEST_DB, '-u', TEST_URI, '-k', TEST_KEY]
            )
        assert result.exit_code != 0
        assert 'Missing option "--collection-name"' in result.output

    # Tests that connection string or primary key/URI needs to be present
    def test_connection_string_uri_and_primary_key_absence(self):
        result = RUNNER.invoke(
            upload, [GOOD_CSV, '--type', CSV_TYPE, '--database-name', TEST_DB,
                     '--collection-name', TEST_COLLECTION]
            )
        assert result.exit_code != 0
        assert 'REQUIRED: Connection string OR *both* a URI and a key' in result.output

    # Tests that URI must be present, if connection string not present
    def test_uri_absent_key_present(self):
        result = RUNNER.invoke(
            upload, [GOOD_CSV, '--type', CSV_TYPE, '--database-name', TEST_DB,
                     '--collection-name', TEST_COLLECTION, '-k', TEST_KEY]
            )
        assert result.exit_code != 0
        assert 'REQUIRED: Connection string OR *both* a URI and a key' in result.output

    # Tests that primary key must be present if connection string not present
    def test_primary_key_absent_uri_present(self):
        result = RUNNER.invoke(
            upload, [GOOD_CSV, '--type', CSV_TYPE, '--database-name', TEST_DB,
                     '--collection-name', TEST_COLLECTION, '-u', TEST_URI]
            )
        assert result.exit_code != 0
        assert 'REQUIRED: Connection string OR *both* a URI and a key' in result.output

    # Tests that connection string is correctly parsed
    def test_connection_string_parsing(self):
        invalid_uri = 'AccountEndpoint=invalidEndpoint'
        invalid_account_key = 'AccountKey:invalidKey;'
        invalid_connection_string = invalid_uri + invalid_account_key
        result = RUNNER.invoke(
            upload, [GOOD_CSV, '--type', CSV_TYPE, '--database-name', TEST_DB,
                     '--collection-name', TEST_COLLECTION, '-s', invalid_connection_string]
            )
        assert result.exit_code != 0
        assert 'The connection string is not properly formatted - aborting' in result.output

    # Test that cosmos_client error throwing functionality, if connection to service fails
    @mock.patch('src.cadet.cosmos_client', autospec=True)
    def test_cosmos_client_throws_error(self, mock_cosmos_client):
        MC = MockClient()
        mock_cosmos_client.CosmosClient.side_effect = MC.CosmosClient
        result = RUNNER.invoke(
            upload, [GOOD_TSV, '--type', TSV_TYPE, '--database-name', TEST_DB,
                     '--collection-name', TEST_COLLECTION, '--conn-string', TEST_CONN_STRING]
            )
        assert result.exit_code != 0
        assert 'Authentication failure to Azure Cosmos' in result.output
