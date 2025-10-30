///////////////////////////////////////////////////////////////////////////////
// Get CRAMs from iRODS and convert them to fastq 
// Logic based on mapcloud CRAM downloader/converter
// https://github.com/Teichlab/mapcloud/tree/58b1d7163de7b0b2b8880fad19d722b586fc32b9/scripts/10x/utils
// Author: kp9, bc8, sm42, ab76, ap41
///////////////////////////////////////////////////////////////////////////////


/////////////////////// IMPORTS AND FUNCTIONS///////////////////////////////////////////////
include { IRODS_FINDCRAMS } from './subworkflows/local/irods_findcrams'
include { IRODS_DOWNLOADCRAMS } from './subworkflows/local/irods_downloadcrams'
include { FASTQS2FTP } from './subworkflows/local/fastq2ftp'


def helpMessage() {
    log.info"""
    ==============================
    nf-irods-to-fastq Pipeline
    ==============================
    This Nextflow pipeline retrieves samples from iRODS storage, converts CRAM/BAM files to FASTQ format, 
    and optionally uploads the results to FTP servers. The pipeline supports comprehensive metadata management 
    and provides three main operations: metadata discovery, CRAM-to-FASTQ conversion, and FTP upload.
    
    Usage: nextflow run main.nf [OPTIONS]
    
    == Required Parameters (choose one) ==
        --samples=path/to/samples.csv       Path to CSV/TSV/JSON file with sample information (requires 'sample' or 'sample_id' column)
        --crams=path/to/crams.csv           Path to CSV/TSV file with CRAM information (columns: sample, cram_path, fastq_prefix)
        --fastqs=path/to/fastqs.csv         Path to CSV file containing FASTQ file information (columns: sample, path)
    
    == Operation Flags ==
        --cram2fastq                        Enable CRAM-to-FASTQ conversion (use with --samples or --crams)
        --toftp                             Enable FTP upload (use with --fastqs)
    
    == Optional Parameters ==
        --output_dir=STRING                 Output directory for results (default: "results")
        --publish_mode=STRING               File publishing mode (default: "copy")
        --index_format=STRING               Index format formula for samtools (default: "i*i*")
        --format_atac=BOOLEAN               Apply ATAC-seq specific formatting (default: true)
        --ignore_patterns=STRING            Patterns to ignore when finding CRAMs (default: "*_phix.cram,*yhuman*,*#888.cram")
        --irods_zone=STRING                 iRODS zone to search (default: "seq")
        
    == FTP Parameters (required when using --toftp) ==
        --ftp_host=STRING                   FTP server hostname (default: "ftp-private.ebi.ac.uk")
        --username=STRING                   FTP username  
        --password=STRING                   FTP password
        --ftp_path=STRING                   Target path on FTP server
        
        Note: When using --toftp, you must also provide --fastqs with a CSV file containing FASTQ paths.

    == Examples ==
    
    1. Sample metadata discovery:
        nextflow run main.nf --samples ./examples/samples.csv

    2. Complete pipeline (discovery + conversion):
        nextflow run main.nf --samples ./examples/samples.csv --cram2fastq
        
    3. CRAM-to-FASTQ conversion from existing metadata:
        nextflow run main.nf --cram2fastq --crams metadata/metadata.tsv
        
    4. FTP upload:
        nextflow run main.nf --toftp --fastqs ./examples/fastqs.csv
        
    5. End-to-end pipeline (two-step process):
        # Step 1: Discovery and conversion
        nextflow run main.nf --samples ./examples/samples.csv --cram2fastq
        
        # Step 2: Upload the generated fastqs.csv (after step 1 completes)
        nextflow run main.nf --toftp --fastqs ./results/fastqs.csv

    == Input File Format Examples ==
    
    samples.csv:
        sample,study_title
        4861STDY7135911,Study_Name
        Human_colon_16S8000511,Human_colon_16S
        
    crams.csv:
        sample,cram_path,fastq_prefix
        4861STDY7135911,/seq/24133/24133_1#4.cram,4861STDY7135911_S1_L001
        4861STDY7135911,/seq/24133/24133_2#2.cram,4861STDY7135911_S1_L002
        
    fastqs.csv:
        sample,path
        4861STDY7135911,results/fastqs/4861STDY7135911/4861STDY7135911_S1_L001_I1_001.fastq.gz
        4861STDY7135911,results/fastqs/4861STDY7135911/4861STDY7135911_S1_L001_R1_001.fastq.gz
        
    == System Requirements ==
        - Nextflow: Version 25.04.4 or higher
        - iRODS client (run 'iinit' before starting)
        - Singularity
        - LSF environment with LSB_DEFAULT_USERGROUP set
    ===============================
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
    } else if ((params.toftp || params.fastqs) && (!params.toftp || !params.fastqs || !params.ftp_host || !params.username || !params.password || !params.ftp_path)) {
        error "Please provide --fastqs and all FTP credentials when using --toftp"
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
                error("ERROR: Please make sure that the ${params.samples} file contains a 'sample' or 'sample_id' column")
            // Check that sample column is not empty (if present)
            }else if (sample == null || sample == '') {
                def row_string = row.collect { k, v -> "${k}:${v}" }.join(',')
                error("ERROR: both 'sample' and 'sample_id' values are missing or empty in the ${params.samples} file for the following entry: \"${row_string}\".\nPlease make sure that the file contains a 'sample' or 'sample_id' column with non-empty values.")
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
            .splitCsv(header: true, sep: ',', quote: '"')
            .map { row -> 
                if (!row.containsKey('fastq_prefix') || row.fastq_prefix == null || row.fastq_prefix == '' || !row.containsKey('cram_path') || row.cram_path == null || row.cram_path == '') {
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
            .collectFile(name: 'fastqs.csv', newLine: false, storeDir: params.output_dir, sort: true, keepHeader: true, skip: 1) { meta, fastq -> 
                def header = "sample,path"
                def line = "${meta.sample},${params.output_dir}/fastqs/${meta.sample}/${fastq.name}"
                "${header}\n${line}\n"
            }
            .subscribe { __ -> 
                log.info("Fastq file list saved to ${params.output_dir}/fastqs.csv")
            }

        // Add versions files to versions channel
        versions = versions.mix(IRODS_DOWNLOADCRAMS.out.versions)
    }

    // STEP 3: Upload fastq files to FTP
    if (params.toftp && params.fastqs) {
        // Read fastq files from input and collect fastq files by sample
        fastqs = Channel.fromPath(params.fastqs, checkIfExists: true)
            .splitCsv(header: true, sep: ',')
            .map {row ->  tuple( [id: row.sample], file(row.path) ) }
            .groupTuple(sort: true)

        // Upload fastq files to FTP
        FASTQS2FTP(
            fastqs,
            params.username,
            params.password,
            params.ftp_host,
            params.ftp_path
        )

        // Collect md5 checksums
        FASTQS2FTP.out.fastqs
            .collectFile(name: 'md5checksums.txt', storeDir: params.output_dir, sort: true, newLine: true) { _meta, fastq, md5 -> 
                "${fastq.name} ${md5}"
            }
            .subscribe { __ -> 
                log.info("MD5 checksums saved to ${params.output_dir}/md5checksums.txt")
            }

        // Attach versions.yml files to the channel
        versions = versions.mix(FASTQS2FTP.out.versions)
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