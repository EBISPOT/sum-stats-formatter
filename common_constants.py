SNP_DSET = 'variant_id'
MANTISSA_DSET = 'mantissa'
EXP_DSET = 'exponent'
PVAL_DSET = 'p_value'
STUDY_DSET = 'study_accession'
CHR_DSET = 'chromosome'
BP_DSET = 'base_pair_location'
OR_DSET = 'odds_ratio'
RANGE_DSET = 'confidence_interval'
BETA_DSET = 'beta'
SE_DSET = 'standard_error'
EFFECT_DSET = 'effect_allele'
OTHER_DSET = 'other_allele'
FREQ_DSET = 'effect_allele_frequency'

vlen_dtype = h5py.special_dtype(vlen=str)
DSET_TYPES = {SNP_DSET: vlen_dtype, PVAL_DSET: vlen_dtype, MANTISSA_DSET: float, EXP_DSET: int,
              STUDY_DSET: vlen_dtype,
              CHR_DSET: int, BP_DSET: int, OR_DSET: float, RANGE_DSET: vlen_dtype,
              BETA_DSET: float, SE_DSET: float,
              EFFECT_DSET: vlen_dtype, OTHER_DSET: vlen_dtype, FREQ_DSET: float}

REFERENCE_DSET = MANTISSA_DSET


TO_LOAD_DSET_HEADERS_DEFAULT = {SNP_DSET, PVAL_DSET, CHR_DSET, BP_DSET, OR_DSET, RANGE_DSET, BETA_DSET,
                        SE_DSET, EFFECT_DSET, OTHER_DSET, FREQ_DSET}
TO_STORE_DSETS_DEFAULT = {SNP_DSET, MANTISSA_DSET, EXP_DSET, STUDY_DSET, CHR_DSET, BP_DSET, OR_DSET, RANGE_DSET,
                  BETA_DSET, SE_DSET, EFFECT_DSET, OTHER_DSET, FREQ_DSET}
TO_QUERY_DSETS_DEFAULT = {SNP_DSET, MANTISSA_DSET, EXP_DSET, STUDY_DSET, CHR_DSET, BP_DSET, OR_DSET, RANGE_DSET, BETA_DSET,
                  SE_DSET, EFFECT_DSET, OTHER_DSET, FREQ_DSET}
