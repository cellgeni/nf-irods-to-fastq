include { findCrams } from './modules/metatable.nf'
include { getMetadata } from './modules/metatable.nf'
include { parseMetadata } from './modules/metatable.nf'
include { combineMetadata } from './modules/metatable.nf'
include { downloadCram } from './modules/getfiles.nf'
include { cramToFastq } from './modules/getfiles.nf'



workflow findcrams {
    main:
        // We need some sort of sample information to download
        if (params.samples == null) {
            error "Please provide a sample-list file via --sample"
        }
        // read sample names from file
        samples = Channel.fromPath(params.samples, checkIfExists: true).splitCsv().flatten()

        // find all cram files for all samples
        cram_path = findCrams(samples).splitCsv()

        // get metadata for samples
        meta_files = getMetadata(cram_path).groupTuple()

        // parse data
        parsed_meta = parseMetadata(meta_files).flatten()

        // write metadata to csv file
       combineMetadata(parsed_meta.collect())
    emit:
        combineMetadata.out
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
    workflow.onComplete {
        log.info "Workflow completed at: ${workflow.complete}"
        log.info "Time taken: ${workflow.duration}"
        log.info "Execution status: ${workflow.success ? 'success' : 'failed'}"
        log.info "Error: $workflow.errorMessage"
    }

}

workflow {
    if (params.from_meta != null) {
        // load existing metadata file
        cram_metadata = Channel.fromPath(params.filtered_meta, checkIfExists: true).splitCsv( header: true , sep: '\t')
    }
    else {
        // find all files and their metadata for a given list of samples
        findcrams()
        cram_metadata = findcrams.out.splitCsv( header: true , sep: '\t')
    }

    // download cram data
    if (params.only_meta == false) {
         downloadcrams(cram_metadata)
    }
}