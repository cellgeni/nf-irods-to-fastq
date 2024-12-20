// functrion with error message if there are no files for a sample
def findCramsError(sample) {
    log.warn "No files found for sample $sample"
    return 'ignore'
}

// Prepare a list of the CRAMs for the sample
// Store the sample ID and the CRAM path in a CSV file for subsequent merging
process findCrams {
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
    input:
        tuple val(sample), val(cram_path)
    output:
        tuple val(sample), path("*.txt")
    script:
        """
        imeta ls -d $cram_path > metadata.txt
        """
}

// Parse metadata for each sample
process parseMetadata {
    input:
        tuple val(sample), path("input/*.txt")
    output:
        path('*.json')
    script:
        """
        parser.py ./input/
        """
}

// Save all metadata to csv file and collect all warnings to .log file
// The warnings a colored using ANSI escape sequences. So sed command matches
// the ANSI escape sequences used for colors. \x1b is the escape character (ESC),
// [ indicates the start of a control sequence, and [0-9;]* captures numbers separated
// by semicolons. m is the final character of the sequence that applies the style.
process combineMetadata {
    input:
        path('input/*.json')
    output:
        path("*.tsv"), emit: metadata
        path('*.log'), emit: log
    script:
        if (task.process == 'FINDMETA:combineMetadata') {
            """
            combine_meta.py ./input/
            """
        } else if (task.process == 'DOWNLOADCRAMS:updateMetadata'){
            """
            combine_meta.py --validate_all --logfile loadcrams.log --filename metadata_final.tsv ./input/
            """
        } else {
            error "Invalid process name: $task.process"
        }
}