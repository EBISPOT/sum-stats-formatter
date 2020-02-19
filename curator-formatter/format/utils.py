import sys
import csv

csv.field_size_limit(sys.maxsize)


known_header_transformations = {

    # variant id
    'snp': 'snp',
    '#SNP': 'snp',
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
    'phase1_1kg_id':'snp',
    'SNP': 'snp',
    'íd': 'snp',
    'MarkerName': 'snp',
    'rsID': 'snp',
    'RSID': 'snp',
    'MARKERNAME': 'snp',    
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
    'bcac_onco_icogs_gwas_p1df': 'pval',
    'bcac_icogs1_risk_P1df': 'pval',
    'mainp': 'pvalue',
    'pv-clinical_c_k57': 'pval',
    'frequentist_add_wald_pvalue_1': 'pval',
    'P':'pval',
    'P.value': 'pval',
    'ALL.RANDOM.PVAL': 'pval',
    'P_BOLT_LMM': 'pval',
    'P_fathers_age_death': 'pval',
    'P_parents_age_death': 'pval',
    'P_top_1_percent': 'pval',
    'P-value' : 'pval',
    'P-val': 'pval',
    'Pvalue': 'pval',
    'p':'pval',
    # chromosome
    'chr': 'chr',
    'chromosome': 'chr',
    'chrom': 'chr',
    'scaffold': 'chr',
    'chr_build36': 'chr',
    '#chrom': 'chr',
    'CHR': 'chr',
    'Chromosome': 'chr',
    # base pair location
    'bp': 'bp',
    'pos': 'bp',
    'position': 'bp',
    'phys_pos': 'bp',
    'base_pair': 'bp',
    'basepair': 'bp',
    'base_pair_location': 'bp',
    'pos_build36': 'bp',  
    'position_b37': 'bp',
    'bp_hg19': 'bp',
    'BP': 'bp',
    'pos(b37)': 'bp',
    'POS_b37': 'bp',
    'POS' : 'bp',
    'Position': 'bp',
    'position_build36': 'bp', 
    'Position_b37': 'bp',
    'Position_hg19': 'bp',
    'Pos_GRCh37': 'bp',
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
    'Chr': 'chr',
    # odds ratio
    'or': 'or',
    'odds_ratio': 'or',
    'oddsratio': 'or',
    'bcac_icogs1_or': 'or',
    'OR': 'or',
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
    'stdbeta': 'beta',
    'bcac_onco_icogs_gwas_beta':'beta',  
    'bcac_icogs1_risk_beta': 'beta',
    'log_odds': 'beta',
    'maineffects': 'beta',
    'nbeta-clinical_c_k57': 'beta',
    'Effect': 'beta',
    'frequentist_add_beta_1:add/sle=1': 'beta',
    'ALL.RANDOM.BETA': 'beta',
    'BETA':'beta',
    'BETA_fathers_age_death': 'beta',
    'BETA_parents_age_death': 'beta',
    'BETA_top_1_percent': 'beta',
    'Beta': 'beta',
    'EFFECT': 'beta',
    'EFFECT_A1': 'beta',
    'beta_SNP_add': 'beta',
    # standard error
    'se': 'se',
    'standard_error': 'se',
    'stderr': 'se',
    'european_ancestry_se_fix': 'se',
    'bcac_onco_icogs_gwas_se': 'se',
    'bcac_icogs1_risk_se': 'se',
    'log_odds_se': 'se',
    'mainse': 'se',
    'standarderror': 'se',
    'nse-clinical_c_k57': 'se',
    'frequentist_add_se_1': 'se',
    'StdErr': 'se',
    'SE': 'se',
    'SE_fathers_age_death': 'se',
    'SE_parents_age_death': 'se',
    'SE_top_1_percent': 'se',
    'StdErr': 'se',    
    'SEBETA': 'se',
    'STDERR': 'se',
    'se_error': 'se',
    'sebeta_SNP_add': 'se',
    'stderr': 'se'
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
    'effectallele': 'effect_allele',
    'a1': 'effect_allele',
    'alleleB': 'effect_allele',
    'A1': 'effect_allele',
    'Allele1':'effect_allele',
    'alleleB': 'effect_allele',
    'ALLELE1': 'effect_allele',
    'EFF_ALLELE': 'effect_allele',
    'EffectAllele': 'effect_allele',
    'coded_allele': 'effect_allele',
    'Coded': 'effect_allele',
    'Effect-allele': 'effect_allele',
    'Effect_allele':'effect_allele',
    # other allele
    'a2': 'other_allele',
    'Allele2': 'other_allele',
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
    'referenceallele': 'other_allele',
    'a0': 'other_allele',
    'noneffect_allele': 'other_allele',
    'alleleB': 'other_allele',
    'A2': 'other_allele',
    'alleleA': 'other_allele',
    'ALLELE0': 'other_allele',  
    'allele2': 'other_allele',
    'NONEFF_ALLELE': 'other_allele',
    'OtherAllele': 'other_allele',
    'non_coded_allele': 'other_allele',    
    'Non_coded': 'other_allele',
    'Other-allele': 'other_allele',
    # effect allele frequency
    'maf': 'eaf',
    'eafcontrols': 'eaf',
    'frq': 'eaf',
    'ref_allele_frequency': 'eaf',
    'frq_u': 'eaf',
    'f_u': 'eaf',
    'effect_allele_freq': 'eaf',
    'effect_allele_frequency': 'eaf',
    'freq1': 'eaf',
    'alt_freq': 'eaf',
    'a1_af': 'eaf',
    'bcac_onco_icogs_gwas_eaf_controls': 'eaf',
    'bcac_icogs1_european_controls_eaf': 'eaf',
    'eaf_ukb': 'eaf',
    'allelefreq': 'eaf', 
    'controls_maf': 'eaf',
    'effectAlleleFreq': 'eaf',
    'ALL.FREQ.VAR': 'eaf',
    'A1FREQ': 'eaf',   
    'freqA1': 'eaf',
    'EAF_UKB': 'eaf',
    'EAF' : 'eaf',
    'EFF_ALLELE_FREQ': 'eaf',
    'Freq': 'eaf',
    'FREQ_A1': 'eaf',
    'Coded_freq': 'eaf',
    'Effect-allele-frequency': 'eaf',
    'Freq1': 'eaf',
    'Effect_allele_frequency': 'eaf',
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
    'cases_total': 'n_cas',
    'ncontrol': 'n_con',
    'controls_n': 'n_con',
    'n_control': 'n_con',
    'weight': 'n',
    'ncompletesamples': 'n',
    'controls_total': 'n_con',
    'N' : 'n',
    'N-analyzed': 'n',
    'TotalN': 'n',
    'Sample-size': 'n',
    'Sample-size-cases': 'n_cas',
    'SampleSize': 'n',
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

VALID_CHROMS = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', 'X', 'Y', 'MT']


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

