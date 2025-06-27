include { concatFastqs } from '../../../modules/upload2ftp.nf'
include { uploadFTP } from '../../../modules/upload2ftp.nf'
include { getSampleName } from '../../../modules/upload2ftp.nf'

workflow UPLOAD2FTP {
    take:
        fastq_ch
    main:
        // merge fastq files together for each sample
        merged_fastq = concatFastqs(fastq_ch)
        
        // upload files to ftp server
        uploadFTP(merged_fastq)
}