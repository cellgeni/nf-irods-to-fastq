include { downloadCram } from '../../../modules/getfiles.nf'
include { cramToFastq } from '../../../modules/getfiles.nf'
include { calculateReadLength } from '../../../modules/getfiles.nf'
include { saveMetaToJson } from '../../../modules/getfiles.nf'
include { combineMetadata } from '../../../modules/metatable.nf'
include { checkATAC } from '../../../modules/getfiles.nf'
include { renameATAC } from '../../../modules/getfiles.nf'


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

        // combine metadata file
        metadata = combineMetadata(json_ch)
    emit:
        combined_fastq
}