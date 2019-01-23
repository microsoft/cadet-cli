from click.testing import CliRunner
from ..cadet import cadet

def test_version():
    runner = CliRunner()
    result = runner.invoke(cadet, ['--version'])
    assert result.exit_code == 0
    assert result.output == 'cadet, version 1.0.0\n'
