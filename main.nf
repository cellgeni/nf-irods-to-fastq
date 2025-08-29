///////////////////////////////////////////////////////////////////////////////
// Get CRAMs from iRODS and convert them to fastq 
// Logic based on mapcloud CRAM downloader/converter
// https://github.com/Teichlab/mapcloud/tree/58b1d7163de7b0b2b8880fad19d722b586fc32b9/scripts/10x/utils
// Author: kp9, bc8, sm42, ab76, ap41
///////////////////////////////////////////////////////////////////////////////


/////////////////////// IMPORTS AND FUNCTIONS///////////////////////////////////////////////
include { IRODS_FINDCRAMS } from './subworkflows/local/irods_findcrams'
include { IRODS_DOWNLOADCRAMS } from './subworkflows/local/irods_downloadcrams'
include { UPLOAD2FTP } from './subworkflows/local/upload2ftp'


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

    // Init channels
    crams    = Channel.empty()
    versions = Channel.empty()

    // STEP 0: Validate input options
    if (params.help) {
        helpMessage()
        System.exit(0)
    } else if (!params.samples && !params.crams && !params.fastqs) {
        helpMessage()
        error "Please use one of the methods listed above"
    } else if (params.samples && params.crams) {
        error "Please use either --samples or --crams, not both"
    }

    // STEP 1: Find CRAMs on iRODS if sample metadata is specified
    if (params.samples) {
        // Read sample names from file
        metadata = Channel.fromPath(params.samples, checkIfExists: true)
        
        // Split metadata based on file format
        if (params.samples.endsWith('.json')) {
            metadata = metadata.splitJson()
        } else if (params.samples.endsWith('.csv')) {
            metadata = metadata.splitCsv(header: true, sep: ',')
        } else if (params.samples.endsWith('.tsv')) {
            metadata = metadata.splitCsv(header: true, sep: '\t')
        } else {
            log.error("Unsupported metadata file format. Please provide a CSV or JSON file.")
            error("Unsupported metadata file format. Please provide a CSV or JSON file.")
        }

        // Add 'id' key to each metadata map based on 'sample' column
        metadata = metadata.map { row -> 
            def sample = row.sample ?: row.sample_id
            // Check that at least one of sample or sample_id columns is present
            if (!row.containsKey('sample') && !row.containsKey('sample_id')) {
                error("ERROR: Please make sure that the ${params.findmeta} file contains a 'sample' or 'sample_id' column")
            // Check that sample column is not empty (if present)
            }else if (sample == null || sample == '') {
                def row_string = row.collect { k, v -> "${k}:${v}" }.join(',')
                error("ERROR: both 'sample' and 'sample_id' values are missing or empty in the ${params.findmeta} file for the following entry: \"${row_string}\".\nPlease make sure that the file contains a 'sample' or 'sample_id' column with non-empty values.")
            // Check that sample_id column is not empty (if present)
            }
            // Add 'id' key with the sample value
            row + [id: sample]
        }

        // Find cram metadata
        IRODS_FINDCRAMS(metadata, params.ignore_patterns)
        crams = IRODS_FINDCRAMS.out.metadata

        // Add versions files to versions channel
        versions = versions.mix(IRODS_FINDCRAMS.out.versions)
    }
    
    // STEP 2: Download CRAMs from iRODS and convert them to .fastq format
    if (params.cram2fastq || params.crams) {
        // Read CRAM metadata from file if specified and check if it contains all necessary columns
        crams = params.crams ? Channel.fromPath(params.crams, checkIfExists: true) : crams
        crams = crams
            .splitCsv(header: true, sep: ',')
            .map { row -> 
                if (row.fastq_prefix == null || row.fastq_prefix == '' || row.cram_path == null || row.cram_path == '') {
                    def row_string = row.collect { k, v -> "${k}:${v}" }.join(',')
                    error "CRAM metadata is missing 'fastq_prefix' or 'cram_path' for CRAM: ${row_string}"
                }
                row.id = row.fastq_prefix
                tuple( row, row.cram_path )
            }

        // Download CRAMs from iRODS and convert them to fastq format
        IRODS_DOWNLOADCRAMS(crams)

        // Write fastq files paths to a csv file
        IRODS_DOWNLOADCRAMS.out.fastqs
            .transpose()
            .collectFile(name: 'fastqlist.csv', newLine: false, storeDir: params.output_dir, sort: true, keepHeader: true, skip: 1) { meta, fastq -> 
                def header = "sample,path"
                def line = "${meta.sample},${params.output_dir}/fastqs/${meta.sample}/${fastq.name}"
                "${header}\n${line}\n"
            }
            .subscribe { __ -> 
                log.info("Fastq file list saved to ${params.output_dir}/fastqlist.csv")
            }

        // Add versions files to versions channel
        versions = versions.mix(IRODS_DOWNLOADCRAMS.out.versions)
    }

    // STEP 3: Upload fastq files to FTP
    if (params.toftp || params.fastqs) {
        // Read fastq files from input and collect fastq files by sample
        fastqs = Channel.fromPath(params.fastqs, checkIfExists: true)
            .splitCsv(header: false, sep: ',')
            .map {fastq_path ->  [getSampleName(fastq_path), fastq_path]}
            .groupTuple()

        // Upload fastq files to FTP
        UPLOAD2FTP(fastqs)
    }

    // COLLECT VERSIONS
    versions = versions
        .splitText(by: 20)
        .unique()
        .collectFile(name: 'versions.yml', storeDir: params.output_dir, sort: true)
        .subscribe { __ -> 
                log.info("Versions saved to ${params.output_dir}/versions.yml")
            }

}