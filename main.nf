include { findCrams } from './modules/metatable.nf'
include { getMetadata } from './modules/metatable.nf'
include { parseMetadata } from './modules/metatable.nf'
include { combineMetadata;  combineMetadata as updateMetadata} from './modules/metatable.nf'
include { downloadCram } from './modules/getfiles.nf'
include { cramToFastq } from './modules/getfiles.nf'
include { calculateReadLength } from './modules/getfiles.nf'
include { saveMetaToJson } from './modules/getfiles.nf'


def helpMessage() {
    log.info"""
    =======================
    iRODS to FASTQ pipeline
    =======================
    This pipeline pulls samples from iRODS along with their metadata and converts them to fastq files.
    Usage: nextflow run main.nf [OPTIONS]
        options:
            --samples=path/to/samples.csv       specify a .csv file with sample names to run a metadata search
            --from_meta=path/to/metadata.csv    download files from IRODS listed in .tsv file and convert them to fastq
            --run_all                           run metadata search and load the files in resulting metadata file

    Examples:
        1. Run a metadata search for a specified list of samples:
            nextflow run main.nf --samples examples/samples.csv

        2. Download cram files (specified in metadata.csv) from IRODS and convert them to fastq
            nextflow run main.nf --from_meta metadata/metadata.tsv
        
        3. Run metadata search and load the files from resulting metadata file
            nextflow run main.nf --samples examples/samples.csv --run_all

    == samples.csv format ==
    UK-CIC10690382
    UK-CIC10690383
    ========================
    """.stripIndent()
}


workflow findmeta {
    take:
        samples
    main:
        // find all cram files for all samples
        cram_path = findCrams(samples).splitCsv()

        // get metadata for samples
        meta_files = getMetadata(cram_path).groupTuple()

        // parse data
        parsed_meta = parseMetadata(meta_files).flatten()

        // write metadata to csv file
       combineMetadata(parsed_meta.collect())
    emit:
        combineMetadata.out.metadata
}


workflow downloadcrams {
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

        // save metadata to json file
        saveMetaToJson(length)
        fastq_ch = saveMetaToJson.out.fastq
        json_ch = saveMetaToJson.out.json.collect()

        // update metadata file
        updateMetadata(json_ch)
    emit:
        updateMetadata.out.metadata
}

workflow {
    // We need some sort of sample information to download
    if (params.samples == null && params.from_meta == null) {
        helpMessage()
        error "Please provide a list of samples file via --samples or metadata file via --from_meta"
    }

    if (params.from_meta == null) {
        // read sample names from file
        samples = Channel.fromPath(params.samples, checkIfExists: true).splitCsv().flatten()
        // find cram metadata
        findmeta(samples)
        cram_metadata = findmeta.out.splitCsv( header: true , sep: '\t')
        
    }
    else {
        // load existing metadata file
        cram_metadata = Channel.fromPath(params.from_meta, checkIfExists: true).splitCsv( header: true , sep: '\t')
    }

    // download cram data
    if (params.from_meta != null || params.run_all != null) {
         downloadcrams(cram_metadata)
    }
}