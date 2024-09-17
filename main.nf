include { findCrams } from './modules/metatable.nf'
include { getMetadata } from './modules/metatable.nf'
include { parseMetadata } from './modules/metatable.nf'
include { combineMetadata } from './modules/metatable.nf'
include { downloadCram } from './modules/getfiles.nf'
include { cramToFastq } from './modules/getfiles.nf'



workflow findcrams {
    take:
        samples
    main:
        // find all cram files for all samples
        cram_path = findCrams(samples).splitCsv()

        // get cram meta for all samples


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

}

workflow {
    // We need some sort of sample information to download
    if (params.sample == null) {
        error "Please provide a sample-list file via --sample"
    }
    // read sample names from file
    samples = Channel.fromPath(params.sample, checkIfExists: true).splitCsv().flatten()

    // find all files and their metadata for a given list of samples
    findcrams(samples)
    cram_metadata = findcrams.out.splitCsv( skip: 1 , sep: '\t')
    //cram_metadata.view()
    //.splitCsv( header: ['sample', 'cram_path', 'fastq_name', 'sample_supplier_name', 'library_type', 'total_reads_irods'], skip: 1 , sep: '\t')

    // Download cram data
    downloadcrams(cram_metadata)
}