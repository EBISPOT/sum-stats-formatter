import pytest
import mock
import harmonise_data


class TestHarmoniseData(object):
    def test_isNumber(self):
        assert harmonise_data.isNumber('10')
        assert harmonise_data.isNumber(5)
        assert harmonise_data.isNumber('-10')
        assert not harmonise_data.isNumber('A')

    def test_reverse_complement(self):
        assert harmonise_data.reverse_complement('AACCTTGG') == 'CCAAGGTT'
        assert harmonise_data.reverse_complement('accn/-gtt') == 'AAC-/NGGT'

    def test_map_bp_to_build_via_ensembl(self):
        with mock.patch.object(harmonise_data.EnsemblRestClient, "map_bp_to_build_with_rest", return_value='some data'):
            assert harmonise_data.map_bp_to_build_via_ensembl('','','','') == 'some data'
        with mock.patch.object(harmonise_data.EnsemblRestClient, "map_bp_to_build_with_rest", return_value=None):
            assert harmonise_data.map_bp_to_build_via_ensembl('','','','') == None

    @mock.patch('harmonise_data.dbSNPclient.check_orientation_with_dbsnp')
    def test_check_orientation_plus(self, mock_check_orientation_with_dbsnp):
        mock_check_orientation_with_dbsnp.return_value = '+'
        assert harmonise_data.check_orientation('') == '+'

    @mock.patch('harmonise_data.dbSNPclient.check_orientation_with_dbsnp')
    def test_check_orientation_minus(self, mock_check_orientation_with_dbsnp):
        mock_check_orientation_with_dbsnp.return_value = '-'
        assert harmonise_data.check_orientation('') == '-'

    # does this test really work?
    @mock.patch('harmonise_data.dbSNPclient.check_orientation_with_dbsnp')
    @mock.patch('harmonise_data.EnsemblRestClient.check_orientation_with_rest')
    def test_check_orientation_false(self, mock_check_orientation_with_dbsnp, mock_check_orientation_with_rest):
        mock_check_orientation_with_dbsnp.return_value = False
        mock_check_orientation_with_rest.return_value = '+'
        result = harmonise_data.check_orientation('')
        assert result == '+'

    @mock.patch('harmonise_data.dbSNPclient.check_orientation_with_dbsnp')
    def test_rev_comp_if_needed_minus(self, mock_check_orientation_with_dbsnp):
        mock_check_orientation_with_dbsnp.return_value = '-'
        result = harmonise_data.rev_comp_if_needed('', 'A', 'G')
        assert result == ('T', 'C')

    @mock.patch('harmonise_data.dbSNPclient.check_orientation_with_dbsnp')
    def test_rev_comp_if_needed_plus(self, mock_check_orientation_with_dbsnp):
        mock_check_orientation_with_dbsnp.return_value = '+'
        result = harmonise_data.rev_comp_if_needed('', 'A', 'G')
        assert result == ('A', 'G')

    @mock.patch('harmonise_data.dbSNPclient.check_orientation_with_dbsnp')
    def test_rev_comp_if_needed_false(self, mock_check_orientation_with_dbsnp):
        mock_check_orientation_with_dbsnp.return_value = False
        result = harmonise_data.rev_comp_if_needed('', 'A', 'G')
        assert result == ('NA', 'NA')


class TestEnsemblRestClient(object):
    @mock.patch('time.sleep')
    def test_slow_down_if_needed(self, mock_sleep):
        mock_sleep.return_value = None
        assert harmonise_data.EnsemblRestClient.slow_down_if_needed({'Retry-After': 5})

    def test_set_json_format(self):
        assert harmonise_data.EnsemblRestClient.set_json_format(None) == {'Content-Type': 'application/json'}
        assert harmonise_data.EnsemblRestClient.set_json_format({'Content-Type': 'something/else'}) == {'Content-Type': 'something/else'}
        assert harmonise_data.EnsemblRestClient.set_json_format({'Something-Else': 'is_here'}) == {'Something-Else': 'is_here', 'Content-Type': 'application/json'}

if __name__ == '__main__':
    pytest.main()
