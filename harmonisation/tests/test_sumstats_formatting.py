import formatting_tools.sumstats_formatting as sf

def test_check_for_header_ambigs():
    assert sf.check_for_header_ambigs(['variant_id']) == True
    assert sf.check_for_header_ambigs(['variant']) == False

def test_row_len_is_header_len():
    assert sf.row_len_is_header_len([1, 2, 3], ['a', 'b', 'c']) == True
    assert sf.row_len_is_header_len([1, 2], ['a', 'b', 'c']) == False

