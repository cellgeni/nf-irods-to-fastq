include { findCrams } from './modules/metatable.nf'
include { getMetadata } from './modules/metatable.nf'
include { parseMetadata } from './modules/metatable.nf'
include { combineMetadata } from './modules/metatable.nf'


workflow findcrams {
    main:
        // We need some sort of sample information to download
        if (params.sample == null) {
            error "Please provide a sample-list file via --sample"
        }
        // read sample names from file
        samples = Channel.fromPath(params.sample, checkIfExists: true).splitCsv().flatten()
        
        // find all cram files for all samples
        cram_path = findCrams(samples).splitCsv()

        // get cram meta for all samples


        // get metadata for samples
        meta_files = getMetadata(cram_path).groupTuple()

        // parse data
        parsed_meta = parseMetadata(meta_files).flatten()

        // write metadata to csv file
       combineMetadata(parsed_meta.collect())
}

workflow {
    findcrams()
}