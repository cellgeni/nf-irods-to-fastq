
// functrion with error message if there are no files for a sample
def findCramsError(sample) {
    log.warn "No files found for sample $sample"
    return 'ignore'
}

// Prepare a list of the CRAMs for the sample
// Store the sample ID and the CRAM path in a CSV file for subsequent merging
process findCrams {
    label "easy"
    tag "Searching files for sample $sample"
    errorStrategy {task.exitStatus == 1 ? findCramsError(sample) : 'terminate'}
    maxForks 10
    input:
        val(sample)
    output:
        path("cramlist.csv")
    script:
        """
        imeta qu -z seq -d sample = "${sample}" and target = 1 and type != "fastq" | \
            grep -v "No rows found" | \
            sed 's/collection: //g' | \
            sed 's/dataObj: //g' | \
            grep -v -- '---' | \
            paste -d '/' - - | \
            grep -v "#888.cram" | \
            grep -v "yhuman" | \
            sed "s/^/${sample},/" > "cramlist.csv" || exit 1
        """
}

// Get the metadata for each sample
process getMetadata {
    label "easy"
    tag "Getting metadata for $cram"
    input:
        tuple val(sample), val(cram_path)
    output:
        tuple val(sample), path("*.txt")
    script:
        """
        imeta ls -d $cram_path > metadata.txt
        """
}

// parse metadata for each sample
process parseMetadata {
    debug true
    label "easy"
    tag "Parsing metadata for $sample"
    input:
        tuple val(sample), path("input/*.txt")
    output:
        path('*.json')
    script:
        """
        parser.py ./input/
        """
}

// Save all metadata to csv file
process combineMetadata {
    label "easy"
    tag "Combining the metadata for all files to metadata.csv"
    publishDir "metadata", mode: "copy", overwrite: true
    input:
        path('input/*.json')
    output:
        path("metadata.tsv")
    script:
        """
        combine_meta.py ./input/
        """
}