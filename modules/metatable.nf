///////////////////////////////////////////////////////////////////////////////
// Get CRAMs from iRODS and convert them to fastq 
// Logic based on mapcloud CRAM downloader/converter
// https://github.com/Teichlab/mapcloud/tree/58b1d7163de7b0b2b8880fad19d722b586fc32b9/scripts/10x/utils
// Author: kp9, bc8, sm42
 ///////////////////////////////////////////////////////////////////////////////



// Prepare a list of the CRAMs for the sample
// Store the sample ID and the CRAM path in a CSV file for subsequent merging
process findCrams {
    label "easy"
    maxForks 10
    input:
        val(sample)
    output:
        path("cramlist.csv")
    script:
        """
        imeta qu -z seq -d sample = "${sample}" and target = 1 | \
            grep -v "No rows found" | \
            sed 's/collection: //g' | \
            sed 's/dataObj: //g' | \
            grep -v -- '---' | \
            paste -d '/' - - | \
            grep -v "#888.cram" | \
            grep -v "yhuman" | \
            sed "s/^/${sample},/" > "cramlist.csv" || echo "WARNING! No files for $sample"
        """
}

// Get the metadata for each sample
process getMetadata {
    label "easy"
    input:
        tuple val(sample), val(cram)
    output:
        path("*.json")
    script:
        """
        imeta ls -d $cram > metadata.txt
        echo $cram
        parser.py metadata.txt
        """
}

// Save all metadata to csv file
process combineMetadata {
    label "easy"
    publishDir "results", mode: "copy"
    input:
        path('input/*.json')
    output:
        path("metadata.csv")
    script:
        """
        combine_meta.py ./input/
        """
}