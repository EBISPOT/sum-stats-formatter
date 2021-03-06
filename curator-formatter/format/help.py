
def main():
    print("\n")
    print("------------------------------------------")
    print("\n")
    print("You can perform the following actions...")
    print("\n")
    print("\t- \'peek -f <filename>\' : will give you a peek into the file you specify")
    print("\n")
    print("\t- \'valid-headers\' : will print out the known valid column headers for inspiration")
    print("\n")
    print("\t- \'format -f <filename>\' : will try to automatically format the file")
    print("\n")
    print("\t- \'rename -f <filename>\' -old <old name> -new <new name>: will rename the header given in the ")
    print("\t\tparamater \'-old\' to the header in the parameter \'-new\'")
    print("\n")
    print("\t- \'merge -f <filename>\' : -left <first header> -right <second header> -d <delimiter> -new <new header>:")
    print("\t\twill merge the columns of left header and right header into one column using the delimiter provided")
    print("\t\tThe new column header will be the one specified with the parameter \'-new\'")
    print("\n")
    print("\t- \'clean -f <filename>\' : -header <header> -fixture <fixture>: will remove the sequence given as a")
    print("\t\tfixture from the data in the column specified")
    print("\n")
    print("\t- \'split -f <filename>\' -header <header> -left <left header> -right <right header> -d <delimiter>:")
    print("\t\twill split the column specified in \'-header\' using the delimiter provided")
    print("\t\tthe split columns will be renamed using the parameters \'-left\' for the value on the left")
    print("\t\t(after the split), and \'-right\' for the value on the right (after the split)")
    print("\n")
    print("\t- \'swap -f <filename>\' -left <left header> -right <right header>")
    print("\t\twill swap the data of left header and right header")
    print("\n")
    print("\t- \'allele-swap -f <filename>\' -header <header>")
    print("\t\twill swap the effect allele and other allele when the value in specified column is FALSE")
    print("\n")
    print("\t- \'delete -f <filename>\' -headers <comma separated list of headers>")
    print("\t\twill remove all the columns of the specified headers from the file")
    print("\n")
    print("\t- \'rename-file -f <filename>\' -efo <efo trait> -study <study accession> -b <genome build> -pmid <pmid> "
          "-author <author>")
    print("\t\trename the file as: author_pmid_studyaccession_efotrait_genomebuild.tsv")
    print("\n")
    print("\t- \'compress -f <filename>\'")
    print("\t\twill compress the file into a .tar.gz archive")
    print("\n")
    print("------------------------------------------")


if __name__ == "__main__":
    main()