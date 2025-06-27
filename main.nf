///////////////////////////////////////////////////////////////////////////////
// Get CRAMs from iRODS and convert them to fastq 
// Logic based on mapcloud CRAM downloader/converter
// https://github.com/Teichlab/mapcloud/tree/58b1d7163de7b0b2b8880fad19d722b586fc32b9/scripts/10x/utils
// Author: kp9, bc8, sm42, ab76, ap41
///////////////////////////////////////////////////////////////////////////////


/////////////////////// IMPORTS AND FUNCTIONS///////////////////////////////////////////////
include { downloadCram } from './modules/getfiles.nf'
include { cramToFastq } from './modules/getfiles.nf'
include { calculateReadLength } from './modules/getfiles.nf'
include { saveMetaToJson } from './modules/getfiles.nf'
include { checkATAC } from './modules/getfiles.nf'
include { renameATAC } from './modules/getfiles.nf'
include { concatFastqs } from './modules/upload2ftp.nf'
include { uploadFTP } from './modules/upload2ftp.nf'
include { getSampleName } from './modules/upload2ftp.nf'


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

workflow DOWNLOADCRAMS {
    take:
        cram_metadata
    main:
        // download cram files
        crams = downloadCram(cram_metadata)
        // crams.cram_file.view()
        
        // convert cram files to fastq
        fastq_files = cramToFastq(crams)
                                .map { fastqfiles, meta, num_reads_processed -> [fastqfiles, meta + ['num_reads_processed': num_reads_processed]] }

        // calculate read length
        length = calculateReadLength(fastq_files)
                                .map { fastqfiles, meta, r1len, r2len, i1len, i2len -> [fastqfiles, meta + ['r1len': r1len, 'r2len': r2len, 'i1len': i1len, 'i2len': i2len]] }

        // rename 10X ATAC files if there are such
        length.branch {
            fastq, meta ->
            atac: checkATAC(meta['library_type'], meta['i2len'], meta['fastq_prefix'])
            other: true
        }
        .set { fastq_ch }

        renameATAC(fastq_ch.atac)

        combined_fastq = fastq_ch.other.concat(renameATAC.out)
        
        // save metadata to json file
        json_ch = saveMetaToJson(combined_fastq).collect()

        // update metadata file
        metadata = updateMetadata(json_ch)
    emit:
        combined_fastq
    publish:
        combined_fastq >> "."

        
}

workflow UPLOADTOFTP {
    take:
        fastq_ch
    main:
        // merge fastq files together for each sample
        merged_fastq = concatFastqs(fastq_ch)
        
        // upload files to ftp server
        uploadFTP(merged_fastq)
}


/////////////////////// MAIN WORKFLOW ///////////////////////////////////////////////
workflow {
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
        samples = Channel.fromPath(params.findmeta, checkIfExists: true).splitCsv().flatten()
        // find cram metadata
        FINDMETA(samples)
        cram_metadata = FINDMETA.out.splitCsv( header: true , sep: '\t')
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
        UPLOADTOFTP(fastq_ch)
    }
}

/////////////////////// WORKFLOW OUTPUT DEFINITION ///////////////////////////////////////////////
// It look stupid (and it is really stupid), but it is the way it work in current nextflow versions

output {

}