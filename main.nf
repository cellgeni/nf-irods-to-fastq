///////////////////////////////////////////////////////////////////////////////
// Get CRAMs from iRODS and convert them to fastq 
// Logic based on mapcloud CRAM downloader/converter
// https://github.com/Teichlab/mapcloud/tree/58b1d7163de7b0b2b8880fad19d722b586fc32b9/scripts/10x/utils
// Author: kp9, bc8, sm42, ab76, ap41
///////////////////////////////////////////////////////////////////////////////


/////////////////////// IMPORTS AND FUNCTIONS///////////////////////////////////////////////
include { IRODS_FINDCRAMS } from './subworkflows/local/irods_findcrams/main.nf'
include { DOWNLOADCRAMS } from './subworkflows/local/downloadcrams/main.nf'
include { UPLOAD2FTP } from './subworkflows/local/upload2ftp/main.nf'


def helpMessage() {
    log.info"""
    =======================
    iRODS to FASTQ pipeline
    =======================
    This pipeline pulls samples from iRODS along with their metadata and converts them to fastq files.
    Usage: nextflow run main.nf [OPTIONS]
        options:
            --findmeta=path/to/samples.csv       specify a .csv file with sample names to run a metadata search
            --cram2fastq                         if specified the script runs conversion of cram files that are found on `findmeta` step
            --meta=path/to/metadata.tsv          this argument spicifies the .tsv with cram files (potentially from `findmeta` step) to run cram2fastq conversion
            --toftp                              if specified the script uploads the data to ftp server specified in nextflow.config file
            --fastqfiles                         this argument spicifies the .fastq.gz files (potentially from `cram2fastq` step) to upload them to ftp server

    Examples:
        1. Run a metadata search for a specified list of samples:
            nextflow run main.nf --findmeta ./examples/samples.csv

        2. Download cram files (specified in metadata.csv) from IRODS and convert them to fastq
            nextflow run main.nf --cram2fastq --meta metadata/metadata.tsv
        
        3. Upload fastq files to ftp server (you to set up the ftp server in nextflow.config):
            nextflow run main.nf --toftp --fastqfiles ./results/
        
        4. Combine several steps to run them together
            nextflow run main.nf --findmeta ./examples/samples.csv --cram2fastq --toftp
        

    == samples.csv format ==
    UK-CIC10690382
    UK-CIC10690383
    ========================
    """.stripIndent()
}

// Get a Sample name from fastq path if fastq_path is in format
// path/to/dir/[Sample Name]_S[Sample Number]_L00[Lane number]_[Read type]_001.fastq.gz
// .*\/(.*?)_: Matches the part of the string after the last / but before _.
// _S.+_L\d{3}: Matches _S followed by the sample number and _L00 followed by three digits (representing the lane number)
// [RI]\d_001\.fastq\.gz: Matches the "Read Type" (R1, R2, I1, I2) and the rest of the file extension.
def getSampleName(fastq_path) {
    def match = fastq_path =~ /.*\/(.*?)_S.+_L\d{3}_[RI]\d_001\.fastq\.gz/
    return match[0][1]
}


/////////////////////// MAIN WORKFLOW ///////////////////////////////////////////////
workflow {
    main:
    // Validate input options
    if (params.help) {
        helpMessage()
        System.exit(0)
    } else if (params.findmeta == null && params.cram2fastq == false && params.toftp == false) {
        helpMessage()
        error "Please use one of the methods listed above"
    // Run findmeta workflow
    } else if (params.findmeta != null) {
        // read sample names from file
        metadata = Channel.fromPath(params.findmeta, checkIfExists: true)
        
        // Split metadata based on file format
        if (params.findmeta.endsWith('.json')) {
            metadata = metadata.splitJson()
        } else if (params.findmeta.endsWith('.csv')) {
            metadata = metadata.splitCsv(header: true, sep: ',')
        } else if (params.findmeta.endsWith('.tsv')) {
            metadata = metadata.splitCsv(header: true, sep: '\t')
        } else {
            log.error("Unsupported metadata file format. Please provide a CSV or JSON file.")
            error("Unsupported metadata file format. Please provide a CSV or JSON file.")
        }

        // Add 'id' key to each metadata map based on 'sample' column
        metadata = metadata.map { row -> 
            // Check that sample column is not empty
            if (!row.containsKey('sample') || row.sample == null || row.sample.trim() == '') {
                def row_string = row.collect { k, v -> "${k}:${v}" }.join(',')
                error("ERROR: 'sample' column is missing or empty in the ${params.findmeta} file for the following entry: \"${row_string}\".\nPlease ensure that the 'sample' column is present and contains valid sample names.")
            }
            // Add 'id' key with the same value as 'sample'
            row + [id: row.sample]
        }

        // find cram metadata
        IRODS_FINDCRAMS(metadata, params.ignore_patterns)
        // cram_metadata = IRODS_FINDCRAMS.out.splitCsv( header: true , sep: '\t')
    // Load metadata from file if specified
    } else if (params.meta != null) {
        // load existing metadata file
        cram_metadata = Channel.fromPath(params.meta, checkIfExists: true).splitCsv( header: true , sep: '\t')
    }
    
    // Run downloadcrams workflow
    if (params.cram2fastq) {
        DOWNLOADCRAMS(cram_metadata)
        fastq_ch = DOWNLOADCRAMS.out.map {fastq_path, meta -> [meta['sample'], fastq_path]}
                                    .transpose()
                                    .groupTuple()
    // Get fastq files from input
    } else if (params.fastqfiles != null) {
        fastq_ch = Channel.fromPath("${params.fastqfiles}/*.fastq.gz")
                      .map {fastq_path ->  [getSampleName(fastq_path), fastq_path]}
                      .groupTuple()
    }
    // Run uploadtoftp workflow
    if (params.toftp) {
        UPLOAD2FTP(fastq_ch)
    }
}