import csv

known_header_transformations = {

    # variant id
    'snp': 'snp',
    'markername': 'snp',
    'marker': 'snp',
    'rs': 'snp',
    'rsid': 'snp',
    'rs_number': 'snp',
    'rs_numbers': 'snp',
    'assay_name': 'snp',
    'id': 'snp',
    'id_dbsnp49': 'snp',
    'snp_rsid': 'snp',
    'MARKER': 'snp',
    'snpid':'snp',
    'oldid':'snp',
    # p-value
    'p': 'pval',
    'pvalue': 'pval',
    'p_value':  'pval',
    'pval': 'pval',
    'p_val': 'pval',
    'gc_pvalue': 'pval',
    'gwas_p': 'pval',
    'frequentist_add_pvalue': 'pval',
    'scan_p': 'pval',
    'scanp': 'pval',
    'p_wald': 'pval', 
    'european_ancestry_pval_fix': 'pval',
    # chromosome
    'chr': 'chr',
    'chromosome': 'chr',
    'chrom': 'chr',
    'scaffold': 'chr',
    'chr_build36': 'chr',
    '#chrom': 'chr',
    # base pair location
    'bp': 'bp',
    'pos': 'bp',
    'position': 'bp',
    'phys_pos': 'bp',
    'base_pair': 'bp',
    'basepair': 'bp',
    'base_pair_location': 'bp',
    'pos_build36': 'bp',  
    # chromosome combined with base pair location
    'chr_pos' : 'chr_bp',
    'chrpos' : 'chr_bp',
    'chrpos_b37' : 'chr_bp',
    'chr_pos_b37' : 'chr_bp',
    'chrpos_b36' : 'chr_bp',
    'chr_pos_b36' : 'chr_bp',
    'chrpos_b38' : 'chr_bp',
    'chr_pos_b38' : 'chr_bp',
    'chr_pos_(b36)' : 'chr_bp',
    'chr_pos_(b37)' : 'chr_bp',
    'chr_pos_(b38)' : 'chr_bp',
    # odds ratio
    'or': 'or',
    'odds_ratio': 'or',
    'oddsratio': 'or',
    # or range
    'L95': 'ci_upper',
    'U95': 'ci_lower',
    'orlower': 'ci_lower',
    'orupper': 'ci_upper',
    'l95': 'ci_lower',
    'u95': 'ci_upper',
    # beta
    'b': 'beta',
    'beta': 'beta',
    'effects': 'beta',
    'effect': 'beta',
    'gwas_beta': 'beta',
    'european_ancestry_beta_fix': 'beta',
    # standard error
    'se': 'se',
    'standard_error': 'se',
    'stderr': 'se',
    'european_ancestry_se_fix': 'se',
    # effect allele
    'a1': 'effect_allele',
    'allele1': 'effect_allele',
    'allele_1': 'effect_allele',
    'effect_allele': 'effect_allele',
    'alt' : 'effect_allele',
    'inc_allele': 'effect_allele',
    'ea': 'effect_allele',
    'alleleb': 'effect_allele',
    'allele_b': 'effect_allele',
    # other allele
    'a2': 'other_allele',
    'allele2': 'other_allele',
    'allele_2': 'other_allele',
    'other_allele': 'other_allele',
    'ref': 'other_allele',
    'non_effect_allele': 'other_allele',
    'dec_allele': 'other_allele',
    'nea': 'other_allele',
    'allelea': 'other_allele',
    'allele_a': 'other_allele',
    'reference_allele': 'other_allele',
    'allele0': 'other_allele',
    # effect allele frequency
    'eaf': 'eaf',
    'maf': 'eaf',
    'eafcontrols': 'eaf',
    'frq': 'eaf',
    'ref_allele_frequency' : 'eaf',
    'frq_u': 'eaf',
    'f_u': 'eaf',
    'effect_allele_freq': 'eaf',
    'effect_allele_frequency': 'eaf',
    'freq1': 'eaf',
    # number of studies
    'nstudy': 'nstudy',
    'n_study': 'nstudy',
    'nstudies': 'nstudy',
    'n_studies': 'nstudy',
    # n
    'n': 'n',
    'ncase': 'n_cas',
    'cases_n': 'n_cas',
    'n_cases': 'n_cas',
    'n_controls': 'n_con',
    'n_cas': 'n_cas',
    'n_con': 'n_con',
    'n_case': 'n_cas',
    'ncontrol': 'n_con',
    'controls_n': 'n_con',
    'n_control': 'n_con',
    'weight': 'n',
    'ncompletesamples': 'n',
    # signed statistics
    'zscore': 'z',
    'z-score': 'z',
    'gc_zscore': 'z',
    'z': 'z',
    'log_odds': 'log_odds',
    'signed_sumstat': 'signed_sumstat',
    # info
    'info': 'info',   
}

CHR_BP = 'chr_bp'
CHR = 'chr'
BP = 'bp'
VARIANT = 'snp'

DESIRED_HEADERS = {'eaf', 'other_allele', 'effect_allele', 'se', 'beta', 'ci_lower', 'ci_upper',
                   'or', 'bp', 'chr', 'pval', 'snp'}
VALID_INPUT_HEADERS = set(known_header_transformations.values())


def get_row_count(file):
    with open(file, 'r') as f:
        return len(f.readlines())


def read_header(file):
    return set([clean_header(x.rstrip('\n')) for x in open(file).readline().split()])


def clean_header(header):
    return header.lower().replace('-', '_').replace('.', '_').replace('\n', '')


def refactor_header(header):
    header = [clean_header(h) for h in header]
    return [known_header_transformations[h] if h in known_header_transformations else h for h in header]


def mapped_headers(header):
    return {h: known_header_transformations[clean_header(h)] for h in header if clean_header(h) in known_header_transformations}


def get_csv_reader(csv_file):
    dialect = csv.Sniffer().sniff(csv_file.readline())
    csv_file.seek(0)
    return csv.reader(csv_file, dialect)


def get_filename(file):
    return file.split("/")[-1].split(".")[0]

