include { findCrams } from '../../../modules/metatable.nf'
include { getMetadata } from '../../../modules/metatable.nf'
include { parseMetadata } from '../../../modules/metatable.nf'
include { combineMetadata} from '../../../modules/metatable.nf'



workflow FINDMETA {
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